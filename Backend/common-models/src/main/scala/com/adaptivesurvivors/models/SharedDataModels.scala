// Backend/common-models/src/main/scala/com/adaptivesurvivors/models/SharedDataModels.scala
package com.adaptivesurvivors.models

/**
 * This case class is the Single Source of Truth for the Seer's feature vector.
 * All Spark jobs will use this model to ensure schema consistency.
 * The Python orchestrator and BigQuery tables will be manually mirrored from this definition.
 *
 * @param run_id The unique ID for the run.
 * @param total_dashes Count of dash events.
 * @param total_damage_dealt Total damage inflicted by the player.
 * @param total_damage_taken Total damage received by the player.
 * @param damage_taken_from_elites Total damage received from elite-type enemies.
 * @param avg_hp_percent The player's average health percentage across the run.
 * @param upgrade_counts A map of upgrade IDs to the number of times they were taken.
 * @param outcome The result of the run (1 for win, 0 for loss, -1 for not applicable). Used for training.
 * @param weight The training weight for this data row (1.0 for bosses, 0.1 for elites).
 */
case class FeatureVector(
  run_id: String,
  total_dashes: Long = 0L,
  total_damage_dealt: Double = 0.0,
  total_damage_taken: Double = 0.0,
  damage_taken_from_elites: Double = 0.0,
  avg_hp_percent: Double = 1.0,
  upgrade_counts: Map[String, Int] = Map.empty,
  outcome: Int = -1,
  weight: Double = 1.0
)