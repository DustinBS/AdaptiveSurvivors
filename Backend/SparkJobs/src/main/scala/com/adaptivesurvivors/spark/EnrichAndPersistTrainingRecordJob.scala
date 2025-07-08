// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/EnrichAndPersistTrainingRecordJob.scala
package com.adaptivesurvivors.spark

import com.adaptivesurvivors.models.FeatureVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * A Spark Streaming job that creates the final, ground-truth training records.
 * It listens for 'boss_fight_completed' events from Kafka, fetches the features cached in HDFS,
 * joins the features with the fight's outcome (win/loss), and persists the resulting
 * enriched record to both the main HDFS data lake and the active BigQuery workspace for immediate use.
 */
object EnrichAndPersistTrainingRecordJob {

  /**
   * Schema for the incoming gameplay event from Kafka.
   * We only care about a few fields from the payload for this specific event type.
   */
  val bossFightEventSchema: StructType = StructType(Seq(
    StructField("run_id", StringType, nullable = false),
    StructField("event_type", StringType, nullable = true),
    StructField("payload", MapType(StringType, StringType), nullable = true)
  ))

  /**
    * The core processing logic for each micro-batch of the streaming query.
    * This function enriches boss fight outcome events with their corresponding run features.
    *
    * @param spark The active SparkSession.
    * @param hdfsFeatureCachePath Path to the HDFS directory where feature vectors are cached.
    * @param hdfsDataLakePath Path to the HDFS directory for the permanent training data lake.
    * @param gcpProjectId Optional GCP Project ID for BigQuery integration.
    * @param bqDataset The BigQuery dataset to write to.
    * @param gcsTempBucket Optional GCS bucket for temporary BigQuery files.
    * @param batchDF The DataFrame for the current micro-batch.
    * @param batchId The ID of the current micro-batch.
    */
  def processBatch(spark: SparkSession, hdfsFeatureCachePath: String, hdfsDataLakePath: String, gcpProjectId: Option[String], bqDataset: String, gcsTempBucket: Option[String])(batchDF: Dataset[Row], batchId: Long): Unit = {
    val logger = Logger.getLogger(getClass.getName)
    val isDryRun = gcpProjectId.isEmpty || gcsTempBucket.isEmpty
    import spark.implicits._

    logger.info(s"--- Processing Enrichment Batch ID: $batchId ---")
    if (!batchDF.isEmpty) {
      // Cache the batch DataFrame for efficiency, as it's accessed multiple times.
      batchDF.cache()

      // --- 1. Read Cached Features from HDFS ---
      // Dynamically construct paths to the feature files based on the run_ids in the current batch.
      val runIds = batchDF.select("run_id").distinct().as[String].collect()
      val featurePaths = runIds.map(id => s"$hdfsFeatureCachePath/run_id=$id/features.json")

      logger.info(s"Found ${runIds.length} boss fights in batch. Reading features from HDFS.")
      // Read all feature files for this batch into a single DataFrame.
      val cachedFeaturesDF = spark.read.json(featurePaths: _*).as[FeatureVector]

      // --- 2. Enrich Features with Outcome ---
      val enrichedDF = batchDF
        .join(cachedFeaturesDF, "run_id")
        // Convert boolean 'win' to integer 'outcome' for machine learning models.
        .withColumn("outcome", when(col("win"), 1).otherwise(0))
        // Select the final set of columns to form the complete training record.
        .select(
          col("run_id"), col("boss_archetype"), col("total_dashes"), col("total_damage_dealt"),
          col("total_damage_taken"), col("damage_taken_from_elites"), col("avg_hp_percent"),
          col("upgrade_counts"), col("outcome"), col("weight")
        )

      logger.info(s"Successfully enriched ${enrichedDF.count()} records.")

      if (!isDryRun) {
        // --- 3a. Sink to Permanent HDFS Data Lake ---
        logger.info(s"Writing enriched data to HDFS Data Lake: $hdfsDataLakePath")
        enrichedDF.write
          // Partition by boss archetype for efficient reads by model training jobs.
          .partitionBy("boss_archetype")
          .mode(SaveMode.Append)
          .json(hdfsDataLakePath)

        // --- 3b. Sink to BigQuery for the current session ---
        spark.conf.set("temporaryGcsBucket", gcsTempBucket.get)

        // Iterate through the archetypes present in this batch and write to the correct BQ table.
        val archetypesInBatch = enrichedDF.select("boss_archetype").distinct().as[String].collect()
        archetypesInBatch.foreach { archetype =>
          val bqTable = s"${gcpProjectId.get}:$bqDataset.${archetype}_training_data"
          logger.info(s"Appending data for archetype '$archetype' to BigQuery table: $bqTable")
          enrichedDF.filter(col("boss_archetype") === archetype)
            .write
            .format("bigquery")
            .option("table", bqTable)
            .mode(SaveMode.Append)
            .save()
        }
      } else {
        logger.warn("DRY RUN: Enriched data that would be written:")
        enrichedDF.show(truncate = false)
      }

      // Release the cached DataFrame from memory.
      batchDF.unpersist()
    } else {
      logger.info("Batch is empty, skipping.")
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)

    val params = args.map { arg =>
      val parts = arg.split("=", 2)
      if (parts.length == 2) (parts(0), parts(1)) else (parts(0), "")
    }.toMap

    // --- Configuration Parameters ---
    val kafkaBootstrapServers = params.getOrElse("--kafka-brokers", "kafka:9092")
    val sourceTopic = params.getOrElse("--source-topic", "gameplay_events")
    val hdfsFeatureCachePath = params.getOrElse("--feature-cache-path", "hdfs://namenode:9000/feature_store/live")
    val hdfsDataLakePath = params.getOrElse("--data-lake-path", "hdfs://namenode:9000/training_data")
    val gcpProjectId = params.get("--gcp-project-id")
    val bqDataset = params.getOrElse("--bq-dataset", "seer_training_workspace")
    val gcsTempBucket = params.get("--gcs-temp-bucket")

    logger.info("--- Initializing Spark Session for Post-Run Enrichment ---")
    val spark = SparkSession.builder()
      .appName("Adaptive Survivors Post-Run Enrichment")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // --- Input Stream from Kafka ---
    val kafkaStreamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", sourceTopic)
      .load()

    // --- Stream Transformation ---
    // Extract and transform the raw Kafka message into a structured DataFrame.
    val bossEventsStream = kafkaStreamDF
      .select(from_json(col("value").cast("string"), bossFightEventSchema).alias("data"))
      .select("data.*")
      .filter(col("event_type") === "boss_fight_completed")
      .withColumn("boss_archetype", col("payload.boss_archetype"))
      .withColumn("win", col("payload.win").cast(BooleanType))
      .select("run_id", "boss_archetype", "win")

    // --- Processing and Sinking ---
    // Apply the batch processing logic to each micro-batch from the stream.
    val query = bossEventsStream.writeStream
      .foreachBatch(processBatch(spark, hdfsFeatureCachePath, hdfsDataLakePath, gcpProjectId, bqDataset, gcsTempBucket) _)
      .start()

    query.awaitTermination()
  }
}