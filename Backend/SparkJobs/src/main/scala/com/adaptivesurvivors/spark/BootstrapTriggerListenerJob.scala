// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/BootstrapTriggerListenerJob.scala
package com.adaptivesurvivors.spark

import com.adaptivesurvivors.spark.TrainingSchemas.BQMLTrainingDataSchema
import com.adaptivesurvivors.models.FeatureVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.ScalaReflection
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

  val playSessionEventSchema: StructType = StructType(
    Seq(StructField("event_type", StringType, nullable = true)))

  /**
   * The core bootstrap logic.
   */
  def runBootstrap(
    spark: SparkSession,
    hdfsPath: String,
    projectId: String,
    dataset: String,
    tempBucket: String,
    logger: Logger): Unit = {
    import spark.implicits._
    logger.info(s"Reading historical training data from: $hdfsPath")

    // Derive the HDFS read schema from the FeatureVector case class to ensure consistency.
    val hdfsSchema = ScalaReflection.schemaFor[FeatureVector].dataType.asInstanceOf[StructType]

    // Read from HDFS. Spark will automatically detect the 'boss_archetype' partition column.
    val historicalDF = Try(spark.read.schema(hdfsSchema).json(hdfsPath))
      .getOrElse(spark.emptyDataFrame)

    historicalDF.cache()

    logger.info(s"Loaded ${historicalDF.count()} historical records. Hydrating BigQuery tables.")
    spark.conf.set("temporaryGcsBucket", tempBucket)

    val archetypeTables = Map(
      "melee" -> "melee_training_data",
      "ranged" -> "ranged_training_data",
      "final" -> "final_training_data")

    archetypeTables.foreach { case (archetype, tableName) =>
      val bqTableId = s"$projectId:$dataset.$tableName"

      // Filter by the partitioned 'boss_archetype' column
      val archetypeDF =
        if (historicalDF.isEmpty || !historicalDF.columns.contains("boss_archetype")) {
          spark.emptyDataFrame
        } else {
          historicalDF.filter(col("boss_archetype") === archetype)
        }

      if (!archetypeDF.isEmpty) {
        // If we have data, THEN we select the columns for BQML.
        val bqmlTrainingDF = archetypeDF.select(BQMLTrainingDataSchema.fieldNames.map(col): _*)
        logger.info(
          s"Writing ${bqmlTrainingDF.count()} records for '$archetype' to $bqTableId with SaveMode.Overwrite")
        bqmlTrainingDF.write
          .format("bigquery")
          .option("table", bqTableId)
          .mode(SaveMode.Overwrite)
          .save()
      } else {
        // If there's no data, we create an empty DataFrame WITH THE CORRECT SCHEMA.
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
    historicalDF.unpersist()
  }

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getName)
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
