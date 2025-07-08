// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/EnrichAndPersistTrainingRecordJob.scala
package com.adaptivesurvivors.spark

import com.adaptivesurvivors.spark.models.FeatureVector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * A Spark Streaming job that creates the final, ground-truth training records.
 * It listens for 'boss_fight_completed' events, fetches the features cached by Flink from HDFS,
 * joins the features with the fight's outcome, and persists the enriched record to both
 * the HDFS data lake and the session's BigQuery workspace.
 */
object EnrichAndPersistTrainingRecordJob.scala {

  // Schema for the incoming gameplay event from Kafka.
  // We only care about a few fields from the payload for this specific event type.
  val bossFightEventSchema: StructType = StructType(Seq(
    StructField("run_id", StringType, nullable = false),
    StructField("event_type", StringType, nullable = true),
    StructField("payload", MapType(StringType, StringType), nullable = true)
  ))

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

    val isDryRun = gcpProjectId.isEmpty || gcsTempBucket.isEmpty

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

    val bossEventsStream = kafkaStreamDF
      .select(from_json(col("value").cast("string"), bossFightEventSchema).alias("data"))
      .select("data.*")
      .filter(col("event_type") === "boss_fight_completed")
      .withColumn("boss_archetype", col("payload.boss_archetype"))
      .withColumn("win", col("payload.win").cast(BooleanType))
      .select("run_id", "boss_archetype", "win")

    // --- Processing Logic per Micro-Batch ---
    val query = bossEventsStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"--- Processing Enrichment Batch ID: $batchId ---")
        if (!batchDF.isEmpty) {
          batchDF.cache() // Cache the batch for multiple uses

          // --- 1. Read Cached Features from HDFS ---
          // Dynamically construct paths to the feature files based on the run_ids in the current batch.
          val runIds = batchDF.select("run_id").distinct().as[String].collect()
          val featurePaths = runIds.map(id => s"$hdfsFeatureCachePath/run_id=$id/features.json")

          logger.info(s"Found ${runIds.length} boss fights in batch. Reading features from HDFS.")
          // Read all feature files for this batch into a single DataFrame. Spark handles missing paths gracefully.
          val cachedFeaturesDF = spark.read.schema(FeatureVector.schema).json(featurePaths: _*)

          // --- 2. Enrich Features with Outcome ---
          val enrichedDF = batchDF
            .join(cachedFeaturesDF, "run_id")
            // Convert boolean 'win' to integer 'outcome' and overwrite the placeholder value from the cache
            .withColumn("outcome", when(col("win"), 1).otherwise(0))
            // Select final columns to match the FeatureVector model, ensuring 'boss_archetype' is included for partitioning.
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
              .partitionBy("boss_archetype") // Partition for efficient reads by the bootstrap job
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

          batchDF.unpersist()
        } else {
          logger.info("Batch is empty, skipping.")
        }
      }
      .start()

    query.awaitTermination()
  }
}