// Backend/common-models/src/main/scala/com/adaptivesurvivors/models/SharedDataModels.scala

package com.adaptivesurvivors.models

/**
 * This case class is the Single Source of Truth for the Seer's feature vector. All Spark and
 * Flink jobs will use this model to ensure schema consistency. The Python orchestrator and
 * BigQuery tables will be manually mirrored from this definition.
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
  weight: Double = 1.0)
