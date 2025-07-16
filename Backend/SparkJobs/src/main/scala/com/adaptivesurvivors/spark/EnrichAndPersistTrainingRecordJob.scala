// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/EnrichAndPersistTrainingRecordJob.scala
package com.adaptivesurvivors.spark

import com.adaptivesurvivors.models.{BQMLTrainingData, FeatureVector}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/** A Spark Streaming job that creates the final, ground-truth training records.
  * It listens for 'boss..' or 'elite_fight_completed' events from Kafka,
  * fetches the features cached in HDFS, joins the features with the fight's
  * outcome (win/loss), and persists the resulting enriched record to both the
  * main HDFS data lake and the active BigQuery workspace for immediate use.
  */
object EnrichAndPersistTrainingRecordJob {

  // Configurable constants for ML training weights, as requested.
  val BOSS_FIGHT_WEIGHT: Double = 1.0
  val ELITE_FIGHT_WEIGHT: Double = 0.1

  // Schema for the incoming boss/elite fight completion events.
  val fightCompletionEventSchema: StructType = StructType(
    Seq(
      StructField("run_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = true),
      StructField("payload", MapType(StringType, StringType), nullable = true)
    )
  )

  /** The core processing logic for each micro-batch of the streaming query.
    * This function enriches boss fight outcome events with their corresponding
    * run features.
    *
    * @param spark
    *   The active SparkSession.
    * @param hdfsFeatureCachePath
    *   Path to the HDFS directory where feature vectors are cached.
    * @param hdfsDataLakePath
    *   Path to the HDFS directory for the permanent training data lake.
    * @param gcpProjectId
    *   Optional GCP Project ID for BigQuery integration.
    * @param bqDataset
    *   The BigQuery dataset to write to.
    * @param gcsTempBucket
    *   Optional GCS bucket for temporary BigQuery files.
    * @param batchDF
    *   The DataFrame for the current micro-batch.
    * @param batchId
    *   The ID of the current micro-batch.
    */
  def processBatch(
      spark: SparkSession,
      hdfsFeatureCachePath: String,
      hdfsDataLakePath: String,
      gcpProjectId: Option[String],
      bqDataset: String,
      gcsTempBucket: Option[String]
  )(batchDF: Dataset[Row], batchId: Long): Unit = {
    val logger = Logger.getLogger(getClass.getName)
    val isDryRun = gcpProjectId.isEmpty || gcsTempBucket.isEmpty
    import spark.implicits._

    logger.info(s"--- Processing Enrichment Batch ID: $batchId ---")
    if (batchDF.isEmpty) {
      logger.info("Batch is empty, skipping.")
      return
    }

    batchDF.cache()

    // --- 1. Read Cached Features from HDFS ---
    // Dynamically construct paths to the feature files based on the run_ids in the current batch.
    val runIds = batchDF.select("run_id").distinct().as[String].collect()
    if (runIds.isEmpty) {
      logger.info("Batch contains no valid run_ids, skipping.")
      batchDF.unpersist()
      return
    }
    val featurePaths =
      runIds.map(id => s"$hdfsFeatureCachePath/run_id=$id/features.json")
    logger.info(
      s"Found ${runIds.length} fights in batch. Reading features from HDFS cache."
    )
    // Define the schema based on your case class
    val featureSchema = spark.implicits.newProductEncoder[FeatureVector].schema
    // Read the JSON files using the explicit schema, then convert to a Dataset
    val cachedFeaturesDF =
      spark.read.schema(featureSchema).json(featurePaths: _*)

    // --- 2. Enrich Features with Outcome ---
    val enrichedDF = batchDF
      .join(cachedFeaturesDF, "run_id")
      // Convert boolean 'win' to integer 'outcome' for machine learning models.
      .withColumn("outcome", when(col("win"), 1).otherwise(0))
      .withColumn(
        "weight",
        when(
          col("event_type") === "boss_fight_completed",
          lit(BOSS_FIGHT_WEIGHT)
        )
          .otherwise(lit(ELITE_FIGHT_WEIGHT))
      )
      // Select the final set of columns to form the complete training record.
      .select(
        col("run_id"),
        col("boss_archetype"),
        col("total_dashes"),
        col("total_damage_dealt"),
        col("total_damage_taken"),
        col("damage_taken_from_elites"),
        col("avg_hp_percent"),
        col("upgrade_counts"),
        col("outcome"),
        col("weight"),
        col("event_type")
      )

    logger.info(s"Successfully enriched ${enrichedDF.count()} records.")

    if (!isDryRun) {
      spark.conf.set("temporaryGcsBucket", gcsTempBucket.get)

      // Sink ALL enriched records (bosses and elites) to BigQuery for in-session model training.
      val archetypesInBatch =
        enrichedDF.select("boss_archetype").distinct().as[String].collect()
      logger.info(
        s"Appending all ${enrichedDF.count()} enriched records to BigQuery."
      )
      archetypesInBatch.foreach { archetype =>
        val bqTable =
          s"${gcpProjectId.get}:$bqDataset.${archetype}_training_data"
        enrichedDF
          .filter(col("boss_archetype") === archetype)
          .drop(
            "event_type",
            "boss_archetype"
          ) // The archetype is implicit in the table name, not a column.
          .write
          .format("bigquery")
          .option("table", bqTable)
          .option("temporaryGcsBucket", gcsTempBucket.get)
          .mode(SaveMode.Append)
          .save()
      }

      // --- HDFS Data Lake Write ---
      // Filter for ONLY boss fights to persist in the permanent HDFS Data Lake.
      val persistentDF =
        enrichedDF.filter(col("event_type") === "boss_fight_completed")
      val persistentCount = persistentDF.count()

      if (persistentCount > 0) {
        logger.info(
          s"Writing ${persistentCount} boss records to permanent HDFS Data Lake: $hdfsDataLakePath"
        )

        // Get the authoritative schema from the case class
        val hdfsSchema = ScalaReflection
          .schemaFor[FeatureVector]
          .dataType
          .asInstanceOf[StructType]

        // Select only the columns that are part of the FeatureVector schema before writing
        persistentDF
          .select(hdfsSchema.fieldNames.map(col): _*)
          .write
          .partitionBy("boss_archetype")
          .mode(SaveMode.Append)
          .json(hdfsDataLakePath)
      } else {
        logger.info(
          "No boss fights in this batch to persist to HDFS Data Lake."
        )
      }

    } else {
      logger.warn("DRY RUN: Enriched data that would be written:")
      enrichedDF.show(truncate = false)
    }
    batchDF.unpersist()
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val logger = Logger.getLogger(getClass.getName)

    // --- Configuration Parameters ---
    val kafkaBootstrapServers =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    val sourceTopic = sys.env.getOrElse(
      "KAFKA_GAMEPLAY_EVENTS_TOPIC",
      "gameplay_events"
    ) // KAFKA_GAMEPLAY_EVENTS_TOPIC not yet setup, but this is fine
    val hdfsFeatureCachePath = sys.env.getOrElse(
      "HDFS_FEATURE_CACHE_PATH",
      "hdfs://namenode:9000/feature_store/live"
    )
    val hdfsDataLakePath = sys.env.getOrElse(
      "HDFS_DATA_LAKE_PATH",
      "hdfs://namenode:9000/training_data"
    )
    val gcpProjectId = sys.env.get("GCP_PROJECT_ID")
    val bqDataset =
      sys.env.getOrElse("SPARK_BQ_DATASET", "seer_training_workspace")
    val gcsTempBucket = sys.env.get("GCS_TEMP_BUCKET")

    logger.info(
      "--- Initializing Spark Session for Sinking In-session Training Data to BigQuery and Sinking to HDFS ---"
    )
    val spark = SparkSession
      .builder()
      .appName("Gameplay-Enrichment-Stream")
      .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
      )
      .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
      )
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
    val fightEventsStream = kafkaStreamDF
      .select(
        from_json(col("value").cast("string"), fightCompletionEventSchema)
          .alias("data")
      )
      .select("data.*")
      .filter(
        col("event_type").isin("boss_fight_completed", "elite_fight_completed")
      )
      .withColumn("boss_archetype", lower(col("payload.boss_archetype")))
      // 'none' is a type meant for special enemies that provide no training data
      .filter(
        col("boss_archetype").isNotNull && col("boss_archetype") =!= "none"
      )
      .withColumn("win", col("payload.win").cast(BooleanType))
      .select("run_id", "boss_archetype", "win", "event_type")

    // --- Processing and Sinking ---
    // Apply the batch processing logic to each micro-batch from the stream.
    val query = fightEventsStream.writeStream
      .foreachBatch(
        processBatch(
          spark,
          hdfsFeatureCachePath,
          hdfsDataLakePath,
          gcpProjectId,
          bqDataset,
          gcsTempBucket
        ) _
      )
      .start()

    query.awaitTermination()
  }
}
