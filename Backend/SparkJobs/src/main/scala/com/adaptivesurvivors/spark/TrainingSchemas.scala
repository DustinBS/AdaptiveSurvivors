// Backend/SparkJobs/src/main/scala/com/adaptivesurvivors/spark/TrainingSchemas.scala

package com.adaptivesurvivors.spark

import org.apache.spark.sql.types._

/**
 * The single source of truth for Spark-specific schemas used across multiple jobs.
 */
object TrainingSchemas {

  /**
   * Defines the schema for the final BQML training tables in BigQuery.
   * It contains only the features and labels required for model training.
   */
  val BQMLTrainingDataSchema: StructType = StructType(Seq(
    // Features
    StructField("total_dashes", LongType, nullable = true),
    StructField("total_damage_dealt", DoubleType, nullable = true),
    StructField("total_damage_taken", DoubleType, nullable = true),
    StructField("damage_taken_from_elites", DoubleType, nullable = true),
    StructField("avg_hp_percent", DoubleType, nullable = true),
    StructField("upgrade_counts", MapType(StringType, IntegerType), nullable = true),

    // Labels / Weights
    StructField("outcome", IntegerType, nullable = true),
    StructField("weight", DoubleType, nullable = true)
  ))
}