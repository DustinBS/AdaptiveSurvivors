// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/HdfsToBigQueryStagingJob.scala
package com.adaptivesurvivors.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, input_file_name, to_date, from_unixtime}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object HdfsToBigQueryStagingJob {

  // Define schemas to ensure data consistency
  // Based on KafkaClient.cs and your debug output
  val gameplayEventSchema: StructType = StructType(Seq(
    StructField("event_type", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = true),
    StructField("player_id", StringType, nullable = true),
    StructField("payload", MapType(StringType, StringType), nullable = true)
  ))

  val adaptiveParamsSchema: StructType = StructType(Seq(
    StructField("playerId", StringType, nullable = true),
    StructField("enemyResistances", MapType(StringType, DoubleType), nullable = true),
    StructField("eliteBehaviorShift", StringType, nullable = true),
    StructField("eliteStatusImmunities", ArrayType(StringType), nullable = true),
    StructField("breakableObjectBuffsDebuffs", MapType(StringType, StringType), nullable = true),
    StructField("timestamp", LongType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    // Suppress verbose Spark INFO logs for cleaner output
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)

    // --- 1. Argument Parsing ---
    val params = args.map { arg =>
      val parts = arg.split("=", 2)
      if (parts.length == 2) (parts(0), parts(1)) else (parts(0), "")
    }.toMap

    // Required HDFS paths
    val gameplayEventsPath = params.getOrElse("--gameplay-events-path", "hdfs://namenode:9000/topics/gameplay_events")
    val adaptiveParamsPath = params.getOrElse("--adaptive-params-path", "hdfs://namenode:9000/topics/adaptive_params")

    // Optional BigQuery parameters
    val gcpProjectId = params.get("--gcp-project-id")
    val bqDataset = params.getOrElse("--bq-dataset", "gameplay_data_staging")
    val gcsTempBucket = params.get("--gcs-temp-bucket")

    logger.info("--- Initializing Spark Session ---")
    val spark = SparkSession.builder()
      .appName("Adaptive Survivors HDFS to BQ Staging")
      .getOrCreate()

    // --- 2. Read Data from HDFS ---
    logger.info(s"Reading gameplay_events from: $gameplayEventsPath")
    val gameplayEventsDF = spark.read
      .schema(gameplayEventSchema)
      .json(s"$gameplayEventsPath/*/*.json") // Wildcard for date and log file partitions

    logger.info(s"Reading adaptive_params from: $adaptiveParamsPath")
    val adaptiveParamsDF = spark.read
      .schema(adaptiveParamsSchema)
      .json(s"$adaptiveParamsPath/*/*.json")

    // --- 3. Transform Data ---
    // Add a 'processing_date' column for future partitioning in BigQuery
    val transformedGameplayDF = gameplayEventsDF
      .withColumn("event_timestamp", (col("timestamp") / 1000).cast(TimestampType))
      .withColumn("processing_date", to_date(col("event_timestamp")))

    val transformedAdaptiveDF = adaptiveParamsDF
      .withColumn("param_timestamp", (col("timestamp") / 1000).cast(TimestampType))
      .withColumn("processing_date", to_date(col("param_timestamp")))

    // --- 4. Write Data (to BigQuery or Console) ---
    val isDryRun = gcpProjectId.isEmpty || gcsTempBucket.isEmpty
    if (isDryRun) {
      logger.warn("!!! GCP Project ID or GCS Temp Bucket not provided. Running in DRY RUN mode. !!!")
      logger.warn("Data will be printed to console instead of written to BigQuery.")

      println("\n--- Transformed Gameplay Events (Sample) ---")
      transformedGameplayDF.show(10, truncate = false)

      println("\n--- Transformed Adaptive Params (Sample) ---")
      transformedAdaptiveDF.show(10, truncate = false)

    } else {
      logger.info(s"--- Writing to BigQuery Project: ${gcpProjectId.get} ---")
      spark.conf.set("temporaryGcsBucket", gcsTempBucket.get)

      val gameplayBqTable = s"${gcpProjectId.get}:$bqDataset.gameplay_events_raw"
      val adaptiveBqTable = s"${gcpProjectId.get}:$bqDataset.adaptive_params_raw"

      // Write Gameplay Events
      logger.info(s"Writing gameplay events to $gameplayBqTable")
      transformedGameplayDF.write
        .format("bigquery")
        .option("table", gameplayBqTable)
        .mode(SaveMode.Append)
        .save()
      logger.info("Successfully wrote gameplay events to BigQuery.")
      
      // Write Adaptive Params
      logger.info(s"Writing adaptive params to $adaptiveBqTable")
      transformedAdaptiveDF.write
        .format("bigquery")
        .option("table", adaptiveBqTable)
        .mode(SaveMode.Append)
        .save()
      logger.info("Successfully wrote adaptive params to BigQuery.")
    }

    logger.info("--- Spark Job Finished ---")
    spark.stop()
  }
}