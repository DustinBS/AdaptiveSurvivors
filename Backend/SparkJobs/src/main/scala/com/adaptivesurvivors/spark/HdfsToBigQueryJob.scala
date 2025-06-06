// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/HdfsToBigQueryJob.scala

// This Spark job reads raw gameplay and adaptive parameters events from HDFS,
// performs schema inference and flattening, and loads them into BigQuery.

package com.adaptivesurvivors.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object HdfsToBigQueryJob {

  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder
      .appName("HdfsToBigQueryJob")
      // Master URL should be configured to point to the Spark Master in docker-compose
      // For local testing, it might be spark://spark-master:7077
      .master(sys.env.getOrElse("SPARK_MASTER_URL", "spark://spark-master:7077"))
      // Configure for HDFS access
      .config("spark.hadoop.fs.defaultFS", sys.env.getOrElse("SPARK_HDFS_NAMENODE_URL", "hdfs://namenode:9000"))
      // BigQuery specific configurations
      // Ensure Google Cloud credentials are available to the Spark container (e.g., via mounted service account key)
      .config("spark.cloud.google.auth.service.account.enable", "true")
      // If running locally with service account JSON, you would typically set:
      // .config("google.cloud.auth.service.account.json.keyfile", "/path/to/your/service-account-key.json")
      // But in Docker, it's better to use GOOGLE_APPLICATION_CREDENTIALS env var or Workload Identity
      .config("spark.jars.packages", "com.google.cloud:spark-bigquery-with-dependencies_2.12:0.26.0")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // --- Configuration ---
    // HDFS path where Kafka Connect stores the data
    val hdfsBasePath = "/user/kafka-connect/topics"
    val gameplayEventsTopicPath = s"${hdfsBasePath}/gameplay_events"
    val adaptiveParamsTopicPath = s"${hdfsBasePath}/adaptive_params"

    // BigQuery dataset and table names
    val projectId = sys.env.getOrElse("GCP_PROJECT_ID", "your-gcp-project-id") // Replace with your GCP Project ID
    val bqDataset = "game_data" // Or whatever you name your BigQuery dataset
    val gameplayEventsStagingTable = s"${projectId}.${bqDataset}.gameplay_events_raw"
    val adaptiveParamsStagingTable = s"${projectId}.${bqDataset}.adaptive_params_raw"

    println(s"Reading gameplay events from HDFS path: $gameplayEventsTopicPath")
    println(s"Writing gameplay events to BigQuery table: $gameplayEventsStagingTable")
    println(s"Reading adaptive parameters from HDFS path: $adaptiveParamsTopicPath")
    println(s"Writing adaptive parameters to BigQuery table: $adaptiveParamsStagingTable")

    // --- Process Gameplay Events ---
    try {
      // Read raw JSON data from HDFS
      val rawGameplayEventsDF = spark.read.json(gameplayEventsTopicPath)

      println("Schema of rawGameplayEventsDF:")
      rawGameplayEventsDF.printSchema()

      // Flatten the payload field.
      // This part requires knowledge of the structure of the 'payload' for each event type.
      // Since 'payload' is a generic map, we'll try to explode it, or access known fields.
      // For a robust solution, you'd typically have a more defined schema for each event type
      // and process them separately or use a UDF to flatten based on event_type.

      // Example of flattening some common fields. This will result in nulls for events that don't have these fields.
      val flattenedGameplayEventsDF = rawGameplayEventsDF
        .withColumn("pos_x", get_json_object(col("payload"), "$.pos.x").cast(DoubleType))
        .withColumn("pos_y", get_json_object(col("payload"), "$.pos.y").cast(DoubleType))
        .withColumn("dir_dx", get_json_object(col("payload"), "$.dir.dx").cast(DoubleType))
        .withColumn("dir_dy", get_json_object(col("payload"), "$.dir.dy").cast(DoubleType))
        .withColumn("weapon_id", get_json_object(col("payload"), "$.weapon_id").cast(StringType))
        .withColumn("dmg_dealt", get_json_object(col("payload"), "$.dmg_dealt").cast(DoubleType))
        .withColumn("enemy_id", get_json_object(col("payload"), "$.enemy_id").cast(StringType))
        .withColumn("dmg_amount", get_json_object(col("payload"), "$.dmg_amount").cast(DoubleType))
        .withColumn("source_type", get_json_object(col("payload"), "$.source_type").cast(StringType))
        .withColumn("lvl", get_json_object(col("payload"), "$.lvl").cast(IntegerType))
        .withColumn("chosen_upgrade_id", get_json_object(col("payload"), "$.chosen_upgrade_id").cast(StringType))
        .withColumn("rejected_ids", get_json_object(col("payload"), "$.rejected_ids").cast(StringType)) // Keep as string for now, array of strings
        .withColumn("enemy_type", get_json_object(col("payload"), "$.enemy_type").cast(StringType))
        .withColumn("killed_by_weapon_id", get_json_object(col("payload"), "$.killed_by_weapon_id").cast(StringType))
        .withColumn("hp", get_json_object(col("payload"), "$.hp").cast(DoubleType))
        .withColumn("mana", get_json_object(col("payload"), "$.mana").cast(DoubleType))
        // active_buffs/debuffs would be complex, keep as string or array of strings
        .withColumn("active_buffs", get_json_object(col("payload"), "$.active_buffs").cast(StringType))
        .withColumn("active_debuffs", get_json_object(col("payload"), "$.active_debuffs").cast(StringType))
        .withColumn("wave", get_json_object(col("payload"), "$.wave").cast(IntegerType))
        .withColumn("time_elapsed", get_json_object(col("payload"), "$.time_elapsed").cast(DoubleType))
        .withColumn("area_explored_percentage", get_json_object(col("payload"), "$.area_explored_percent").cast(DoubleType))
        .withColumn("obj_id", get_json_object(col("payload"), "$.obj_id").cast(StringType))
        .withColumn("obj_type", get_json_object(col("payload"), "$.obj_type").cast(StringType))
        .drop("payload") // Drop the original complex payload column

      println("Schema of flattenedGameplayEventsDF:")
      flattenedGameplayEventsDF.printSchema()

      // Write to BigQuery staging table
      flattenedGameplayEventsDF.write
        .format("bigquery")
        .option("table", gameplayEventsStagingTable)
        .option("temporaryGcsBucket", s"${projectId}-spark-temp-bucket") // Temporary GCS bucket for BigQuery connector
        .mode("append") // Append new data to the table
        .save()

      println(s"Successfully loaded gameplay events to BigQuery table: $gameplayEventsStagingTable")

    } catch {
      case e: Exception =>
        println(s"Error processing gameplay events: ${e.getMessage}")
        e.printStackTrace()
    }

    // --- Process Adaptive Parameters ---
    try {
      // Read raw JSON data from HDFS
      val rawAdaptiveParamsDF = spark.read.json(adaptiveParamsTopicPath)

      println("Schema of rawAdaptiveParamsDF:")
      rawAdaptiveParamsDF.printSchema()

      // Flatten complex fields if necessary, or keep as JSON strings for BigQuery JSON type
      val flattenedAdaptiveParamsDF = rawAdaptiveParamsDF
        // For simplicity, we'll keep maps and sets as JSON strings
        .withColumn("enemyResistances_json", to_json(col("enemyResistances")))
        .withColumn("eliteStatusImmunities_json", to_json(col("eliteStatusImmunities")))
        .withColumn("breakableObjectBuffsDebuffs_json", to_json(col("breakableObjectBuffsDebuffs")))
        .drop("enemyResistances", "eliteStatusImmunities", "breakableObjectBuffsDebuffs")

      println("Schema of flattenedAdaptiveParamsDF:")
      flattenedAdaptiveParamsDF.printSchema()

      // Write to BigQuery staging table
      flattenedAdaptiveParamsDF.write
        .format("bigquery")
        .option("table", adaptiveParamsStagingTable)
        .option("temporaryGcsBucket", s"${projectId}-spark-temp-bucket") // Re-use or create a dedicated GCS bucket
        .mode("append") // Append new data to the table
        .save()

      println(s"Successfully loaded adaptive parameters to BigQuery table: $adaptiveParamsStagingTable")

    } catch {
      case e: Exception =>
        println(s"Error processing adaptive parameters: ${e.getMessage}")
        e.printStackTrace()
    }

    spark.stop()
  }
}
