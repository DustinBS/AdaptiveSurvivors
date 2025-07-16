// Backend/common-models/src/main/scala/com/adaptivesurvivors/models/SharedDataModels.scala
package com.adaptivesurvivors.models
import org.apache.spark.sql.types._

/** This case class is the Source of Truth between the Flink caching job and
  * Spark for the Seer's cached data. Spark jobs will use this model to ensure
  * schema consistency.
  *
  * @param run_id
  *   The unique ID for the run.
  * @param encounterId
  *   The unique ID for the current Seer encounter.
  * @param total_dashes
  *   Count of dash events.
  * @param total_damage_dealt
  *   Total damage inflicted by the player.
  * @param total_damage_taken
  *   Total damage received by the player.
  * @param damage_taken_from_elites
  *   Total damage received from elite-type enemies.
  * @param avg_hp_percent
  *   The player's average health percentage across the run.
  * @param upgrade_counts
  *   A map of upgrade IDs to the number of times they were taken.
  * @param outcome
  *   The result of the run (1 for win, 0 for loss, -1 for not applicable). Used
  *   for training.
  * @param weight
  *   The training weight for this data row (1.0 for bosses, 0.1 for elites).
  */
case class FeatureVector(
    run_id: String,
    encounterId: String,
    total_dashes: Long = 0L,
    total_damage_dealt: Double = 0.0,
    total_damage_taken: Double = 0.0,
    damage_taken_from_elites: Double = 0.0,
    avg_hp_percent: Double = 1.0,
    upgrade_counts: Map[String, Int] = Map.empty,
    outcome: Int = -1,
    weight: Double = 1.0
)

/** The single source of truth for the final BQML training data schema. This
  * object is imported and used by all Spark jobs that interact with the BQML
  * training tables to ensure 100% schema consistency.
  *
  * It contains only the features and labels required for model training.
  */
object BQMLTrainingData {
  val schema: StructType = StructType(
    Seq(
      // Features
      StructField("total_dashes", LongType, nullable = true),
      StructField("total_damage_dealt", DoubleType, nullable = true),
      StructField("total_damage_taken", DoubleType, nullable = true),
      StructField("damage_taken_from_elites", DoubleType, nullable = true),
      StructField("avg_hp_percent", DoubleType, nullable = true),
      StructField(
        "upgrade_counts",
        MapType(StringType, IntegerType),
        nullable = true
      ),

      // Labels / Weights
      StructField(
        "outcome",
        IntegerType,
        nullable = true
      ), // 1 for win, 0 for loss
      StructField("weight", DoubleType, nullable = true)
    )
  )
}
