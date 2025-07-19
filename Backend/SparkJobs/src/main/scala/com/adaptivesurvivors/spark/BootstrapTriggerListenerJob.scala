package com.adaptivesurvivors.spark

import com.adaptivesurvivors.spark.TrainingSchemas.BQMLTrainingDataSchema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.Try

/**
 * A lightweight Spark Streaming job that listens for a single 'play_session_started' event from
 * Kafka. Upon receiving the first event, it triggers a one-time bootstrap process to populate
 * BigQuery from the HDFS data lake, and then stops processing.
 */
object BootstrapTriggerListenerJob {

  // A flag to ensure the bootstrap logic runs only once per application lifetime.
  @volatile private var bootstrapTriggered = false

  // The number of times boss fight records will be duplicated to increase their training weight.
  val BOSS_FIGHT_DUPLICATION_FACTOR: Int = 10

  // The schema for the incoming event that triggers the bootstrap process.
  val playSessionEventSchema: StructType = StructType(
    Seq(StructField("event_type", StringType, nullable = true)))

  /**
   * The core bootstrap logic. It performs an idempotent, one-time seeding of the HDFS data lake
   * with handcrafted data, then reads all historical data, applies oversampling to weight
   * important records, and populates the BigQuery ML tables.
   */
  def runBootstrap(
    spark: SparkSession,
    hdfsPath: String,
    projectId: String,
    dataset: String,
    tempBucket: String,
    logger: Logger): Unit = {
    import spark.implicits._

    // This schema includes partitioning/ID columns plus all fields from the BQML schema.
    // It's used to read the raw data from HDFS before oversampling.
    val hdfsSeedSchema = StructType(
      Seq(
        StructField("boss_archetype", StringType, false),
        StructField("run_id", StringType, false),
        StructField("encounterId", StringType, false),
        StructField("weight", DoubleType, false)) ++ BQMLTrainingDataSchema.fields)

    // --- Step 1: Idempotent Seeding ---
    val markerPath = new org.apache.hadoop.fs.Path(s"$hdfsPath/_SEED_DATA_APPLIED")
    val hdfs = markerPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    if (!hdfs.exists(markerPath)) {
      logger.warn(
        "Seed data marker not found. Performing one-time data seeding from hardcoded values.")

      // Data is ordered to match the hdfsSeedSchema structure.
      // Elite-like records get a weight of 0.1, while Boss-like records get 1.0.
      val seedDataRows = Seq(
        Row("melee", "seed_run_melee_loss", "seed_encounter", 0.1, 5L, 100.0, 150.0, 0.0, 0.3, Map[String, Int](), 0),
        Row("melee", "seed_run_melee_win", "seed_encounter", 0.1, 20L, 500.0, 50.0, 0.0, 0.8, Map[String, Int](), 1),
        Row("ranged", "seed_run_ranged_loss", "seed_encounter", 0.1, 10L, 200.0, 120.0, 0.0, 0.4, Map[String, Int](), 0),
        Row("ranged", "seed_run_ranged_win", "seed_encounter", 0.1, 30L, 600.0, 40.0, 0.0, 0.9, Map[String, Int](), 1),
        Row("final", "seed_run_final_loss", "seed_encounter", 1.0, 15L, 400.0, 200.0, 0.0, 0.2, Map[String, Int](), 0),
        Row("final", "seed_run_final_win", "seed_encounter", 1.0, 40L, 1000.0, 100.0, 0.0, 0.7, Map[String, Int](), 1)
      )

      if (seedDataRows.nonEmpty) {
        val seedDF =
          spark.createDataFrame(spark.sparkContext.parallelize(seedDataRows), hdfsSeedSchema)

        seedDF.write
          .partitionBy("boss_archetype")
          .mode(SaveMode.Append)
          .json(hdfsPath)

        hdfs.create(markerPath).close()
        logger.info(
          "Successfully seeded data lake from hardcoded values and created marker file.")
      }

    } else {
      logger.info("Seed data marker found. Skipping seeding process.")
    }

    // --- Step 2: Main Bootstrap Logic (runs AFTER seeding is guaranteed) ---
    logger.info(s"Reading historical training data from: $hdfsPath")
    val historicalDF = Try(spark.read.schema(hdfsSeedSchema).json(hdfsPath))
      .getOrElse(spark.emptyDataFrame)

    // Apply oversampling to give more weight to boss fights
    val eliteRecordsDF = historicalDF.filter(col("weight") < 1.0)
    val bossRecordsDF = historicalDF.filter(col("weight") >= 1.0)

    val duplicatedBossRecordsDF = (1 until BOSS_FIGHT_DUPLICATION_FACTOR)
      .map(_ => bossRecordsDF)
      .reduceOption(_ union _)
      .getOrElse(spark.emptyDataFrame)

    val oversampledHistoricalDF =
      eliteRecordsDF.unionByName(bossRecordsDF).unionByName(duplicatedBossRecordsDF)
    logger.info(
      s"Loaded and oversampled ${oversampledHistoricalDF.count()} historical records for BigQuery hydration.")
    oversampledHistoricalDF.cache()

    spark.conf.set("temporaryGcsBucket", tempBucket)

    val archetypeTables = Map(
      "melee" -> "melee_training_data",
      "ranged" -> "ranged_training_data",
      "final" -> "final_training_data")

    archetypeTables.foreach { case (archetype, tableName) =>
      val bqTableId = s"$projectId:$dataset.$tableName"
      val archetypeDF = oversampledHistoricalDF.filter(col("boss_archetype") === archetype)

      if (!archetypeDF.isEmpty) {
        // Select only the columns for the BQML model, excluding weight and identifiers
        val bqmlTrainingDF = archetypeDF.select(BQMLTrainingDataSchema.fieldNames.map(col): _*)
        logger.info(
          s"Writing ${bqmlTrainingDF.count()} records for '$archetype' to $bqTableId with SaveMode.Overwrite")

        bqmlTrainingDF.write
          .format("bigquery")
          .option("table", bqTableId)
          .mode(SaveMode.Overwrite)  // Overwrite ensures a clean slate on bootstrap
          .save()
      } else {
        logger.warn(s"No historical data for '$archetype'. Creating empty table at $bqTableId.")
        val emptyDF =
          spark.createDataFrame(spark.sparkContext.emptyRDD[Row], BQMLTrainingDataSchema)
        emptyDF.write
          .format("bigquery")
          .option("table", bqTableId)
          .mode(SaveMode.Overwrite)
          .save()
      }
    }
    oversampledHistoricalDF.unpersist()
  }

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass.getName)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // --- Configuration Parameters ---
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    val sourceTopic = sys.env.getOrElse("KAFKA_GAMEPLAY_EVENTS_TOPIC", "gameplay_events")
    val historicalDataPath =
      sys.env.getOrElse("HDFS_DATA_LAKE_PATH", "hdfs://namenode:9000/training_data")
    val gcpProjectId = sys.env.get("GCP_PROJECT_ID")
    val bqDataset = sys.env.getOrElse("SPARK_BQ_DATASET", "seer_training_workspace")
    val gcsTempBucket = sys.env.get("GCS_TEMP_BUCKET")

    val spark = SparkSession
      .builder()
      .appName("HDFS-to-BigQuery-Bootstrap")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", sourceTopic)
      .load()

    val sessionStartStream = kafkaStreamDF
      .select(from_json(col("value").cast("string"), playSessionEventSchema).alias("data"))
      .select("data.*")
      .filter(col("event_type") === "play_session_started")

    val query = sessionStartStream.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty && !bootstrapTriggered) {
          synchronized {
            if (!bootstrapTriggered) {
              logger.info(
                s"--- 'play_session_started' event received. Triggering data bootstrap. ---")
              bootstrapTriggered = true

              val isDryRun = gcpProjectId.isEmpty || gcsTempBucket.isEmpty
              if (!isDryRun) {
                runBootstrap(
                  spark,
                  historicalDataPath,
                  gcpProjectId.get,
                  bqDataset,
                  gcsTempBucket.get,
                  logger)
              } else {
                logger.warn(
                  "DRY RUN: Bootstrap would have been triggered, but GCP config is missing.")
              }
              logger.info("--- Bootstrap complete. Listener will now idle. ---")
            }
          }
        }
      }
      .start()

    query.awaitTermination()
  }
}
