// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/BootstrapTriggerListenerJob.scala
package com.adaptivesurvivors.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * A lightweight Spark Streaming job that listens for a single 'play_session_started' event
 * from Kafka. Upon receiving the first event, it triggers a one-time bootstrap process to
 * populate BigQuery from the HDFS data lake, and then stops processing.
 */
object BootstrapTriggerListenerJob {

  // A flag to ensure the bootstrap logic runs only once per application lifetime.
  @volatile private var bootstrapTriggered = false

  val playSessionEventSchema: StructType = StructType(Seq(
    StructField("event_type", StringType, nullable = true)
    // We only need to know the event occurred, so schema is minimal.
  ))

  val historicalTrainingDataSchema: StructType = StructType(Seq(
    StructField("run_id", StringType, nullable = false),
    StructField("boss_archetype", StringType, nullable = false), // Critical for splitting data
    StructField("total_dashes", LongType, nullable = true),
    StructField("total_damage_dealt", DoubleType, nullable = true),
    StructField("total_damage_taken", DoubleType, nullable = true),
    StructField("damage_taken_from_elites", DoubleType, nullable = true),
    StructField("avg_hp_percent", DoubleType, nullable = true),
    StructField("upgrade_counts", MapType(StringType, IntegerType), nullable = true),
    StructField("outcome", IntegerType, nullable = true), // 1 for win, 0 for loss
    StructField("weight", DoubleType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(getClass.getName)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val params = args.map { arg =>
        val parts = arg.split("=", 2)
        if (parts.length == 2) (parts(0), parts(1)) else (parts(0), "")
    }.toMap

    // --- Configuration Parameters ---
    val kafkaBootstrapServers = params.getOrElse("--kafka-brokers", "kafka:9092")
    val sourceTopic = params.getOrElse("--source-topic", "gameplay_events")
    val historicalDataPath = params.getOrElse("--historical-data-path", "hdfs://namenode:9000/training_data")
    val gcpProjectId = params.get("--gcp-project-id")
    val bqDataset = params.getOrElse("--bq-dataset", "seer_training_workspace")
    val gcsTempBucket = params.get("--gcs-temp-bucket")

    val spark = SparkSession.builder()
      .appName("Adaptive Survivors Bootstrap Trigger Listener")
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
      .trigger(Trigger.ProcessingTime("10 seconds")) // Check for new events periodically
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty && !bootstrapTriggered) {
          // Synchronize to prevent race conditions in a distributed environment
          synchronized {
            if (!bootstrapTriggered) {
              logger.info(s"--- 'play_session_started' event received. Triggering data bootstrap. ---")
              bootstrapTriggered = true // Set flag immediately

              // --- Execute Bootstrap Logic ---
              val isDryRun = gcpProjectId.isEmpty || gcsTempBucket.isEmpty
              if (!isDryRun) {
                runBootstrap(spark, historicalDataPath, gcpProjectId.get, bqDataset, gcsTempBucket.get, logger)
              } else {
                logger.warn("DRY RUN: Bootstrap would have been triggered, but GCP config is missing.")
              }
              logger.info("--- Bootstrap complete. Listener will now idle. ---")
            }
          }
        }
      }.start()

    query.awaitTermination()
  }

  /**
   * The core bootstrap logic, extracted into a separate function.
   */
  def runBootstrap(spark: SparkSession, hdfsPath: String, projectId: String, dataset: String, tempBucket: String, logger: Logger): Unit = {
    logger.info(s"Reading historical training data from: $hdfsPath")
    val historicalDF = spark.read.schema(historicalTrainingDataSchema).json(s"$hdfsPath/*/*.json")
    historicalDF.cache()

    logger.info(s"Loaded ${historicalDF.count()} historical records. Writing to BigQuery.")
    spark.conf.set("temporaryGcsBucket", tempBucket)

    val archetypeTables = Map("melee" -> "melee_training_data", "ranged" -> "ranged_training_data", "final" -> "final_training_data")
    archetypeTables.foreach { case (archetype, tableName) =>
      val bqTableId = s"$projectId:$dataset.$tableName"
      val archetypeDF = historicalDF.filter(col("boss_archetype") === archetype)

      if (!archetypeDF.isEmpty) {
        logger.info(s"Writing ${archetypeDF.count()} records for '$archetype' to $bqTableId with SaveMode.Overwrite")
        archetypeDF.write
          .format("bigquery")
          .option("table", bqTableId)
          .mode(SaveMode.Overwrite)
          .save()
      } else {
        logger.warn(s"No historical data found for archetype '$archetype'.")
      }
    }
    historicalDF.unpersist()
  }
}