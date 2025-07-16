// Backend/FlinkJobs/src/main/scala/com/adaptivesurvivors/flink/PlayerProfileAndAdaptiveParametersJob.scala
package com.adaptivesurvivors.flink

import com.google.gson.{Gson, GsonBuilder}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import scala.math.sqrt
import scala.util.{Failure, Success, Try}

// --- Data Models for internal logic and Kafka payloads ---
// Represents the raw incoming event from Kafka
case class GameplayEvent(event_type: String, timestamp: Long, player_id: String, run_id: String, payload: java.util.Map[String, AnyRef])

// Unified state object holding data for ALL real-time features
case class PlayerProfile(
  var playerId: String,
  var runId: String,
  // Seer Features
  var totalDashes: Long = 0L,
  var totalDamageDealt: Double = 0.0,
  var totalDamageTaken: Double = 0.0,
  var damageTakenFromElites: Double = 0.0,
  var hpHistory: List[Double] = List.empty,
  var upgradeCounts: Map[String, Int] = Map.empty,
  // Adaptive Enemy Features
  var lastPlayerVelocity: PredictionVector = PredictionVector(0, 0),
  var recentKillDotProducts: List[Double] = List.empty,
  var dashDirectionCounts: Map[String, Int] = Map.empty,
  var lastVexerPrediction: String = "none",
  var currentBruteForm: String = "skirmisher",
  var activeStaleTimer: Long = 0L,
  var stalenessOverrideUntil: Long = 0L,
  var lastUpdated: Long = System.currentTimeMillis()
)

// Payloads for messages sent to the 'adaptive_params' topic
case class AdaptiveMessageEnvelope(message_type: String, payload: String)
case class FormAdaptationPayload(playerId: String, adaptation_type: String)
case class VexerPredictionPayload(playerId: String, predicted_direction: PredictionVector)
case class PredictionVector(dx: Float, dy: Float)

/**
 * Deserializes JSON strings into GameplayEvent objects with robust error handling.
 */
class JsonToGameplayEventMapper extends RichMapFunction[String, GameplayEvent] {
    @transient private var gson: Gson = _
    @transient private var logger: Logger = _

    override def open(parameters: Configuration): Unit = {
        gson = new Gson()
        logger = LoggerFactory.getLogger(classOf[JsonToGameplayEventMapper])
    }
    override def map(jsonString: String): GameplayEvent = {
        try {
            gson.fromJson(jsonString, classOf[GameplayEvent])
        } catch {
            case e: Exception =>
                logger.error(s"Failed to parse JSON, skipping record: $jsonString", e)
                GameplayEvent("invalid_event", 0L, "unknown", "unknown", new java.util.HashMap[String, AnyRef]())
        }
    }
}

/**
 * This Flink job is the central real-time engine for Adaptive Survivors. It consumes all gameplay events and:
 * 1. Continuously generates adaptation parameters for enemies (Brute, Vexer) and sends them to the 'adaptive_params' topic.
 * 2. Emits full player feature vector when triggered by a 'seer_encounter_begin' or 'elite_fight_completed event, and sends it
 * to the 'bqml_features' topic, caches it in HDFS, and triggers the Python ML orchestrator.
 */
object PlayerProfileAndAdaptiveParametersJob {
  val maxDotProductHistory = 20
  val stalenessCheckDurationMs = 5000 // 5 seconds
  val stalenessGracePeriodMs = 5000   // 5 seconds

  // Define OutputTags for Flink's Side Output feature
  val adaptiveParamsOutputTag: OutputTag[String] = OutputTag[String]("adaptive-params")
  val seerFeaturesOutputTag: OutputTag[String] = OutputTag[String]("seer-features")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    val hdfsFeatureCachePath = sys.env.getOrElse("HDFS_FEATURE_CACHE_PATH", "hdfs://namenode:9000/feature_store/live")

    val globalJobParams = new Configuration()
    globalJobParams.setString("HDFS_FEATURE_CACHE_PATH", hdfsFeatureCachePath)
    env.getConfig.setGlobalJobParameters(globalJobParams)

    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(kafkaBootstrapServers)
      .setTopics("gameplay_events")
      .setGroupId("flink-unified-profile-consumer")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val rawEventStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Gameplay Events Kafka Source")

    val processedStream = rawEventStream
      .map(new JsonToGameplayEventMapper())
      .filter(_.event_type != "invalid_event")
      .keyBy(_.run_id) // Key by run_id for per-run state
      .process(new UnifiedProfileProcessor(hdfsFeatureCachePath, adaptiveParamsOutputTag, seerFeaturesOutputTag))

    // --- Kafka Sinks for Each Output Type ---
    val adaptiveParamsSink = createKafkaSink("adaptive_params", kafkaBootstrapServers)
    val seerFeaturesSink = createKafkaSink("bqml_features", kafkaBootstrapServers)
    // Route the side output stream to the adaptive params sink
    processedStream.getSideOutput(adaptiveParamsOutputTag).sinkTo(adaptiveParamsSink).name("Adaptive Params Kafka Sink")

    // Route the main output stream to the seer features sink
    processedStream.getSideOutput(seerFeaturesOutputTag).sinkTo(seerFeaturesSink).name("Seer Features Kafka Sink")

    env.execute("Unified Player Profile & Feature Job")
  }

  def createKafkaSink(topic: String, brokers: String): KafkaSink[String] = {
    KafkaSink.builder[String]()
      .setBootstrapServers(brokers)
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic(topic)
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .build()
  }
}

class UnifiedProfileProcessor(hdfsCachePath: String, adaptiveTag: OutputTag[String], seerTag: OutputTag[String])
  extends KeyedProcessFunction[String, GameplayEvent, Unit] {

    @transient private var profileState: ValueState[PlayerProfile] = _
    @transient private var gson: Gson = _
    @transient private var logger: Logger = _
    @transient private var hdfsFileSystem: org.apache.hadoop.fs.FileSystem = _

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor("unified-player-profile", classOf[PlayerProfile])
      profileState = getRuntimeContext.getState(descriptor)
      gson = new GsonBuilder().create()
      logger = LoggerFactory.getLogger(classOf[UnifiedProfileProcessor])

      val hadoopConf = new org.apache.hadoop.conf.Configuration()
      hadoopConf.set("fs.defaultFS", hdfsCachePath)
      hadoopConf.set("dfs.client.use.datanode.hostname", "true")
      hadoopConf.set("dfs.datanode.use.datanode.hostname", "true")
      hdfsFileSystem = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfsCachePath), hadoopConf)
      logger.info(s"HDFS FileSystem initialized for URI: ${hdfsFileSystem.getUri}")
    }

  override def processElement(event: GameplayEvent, ctx: KeyedProcessFunction[String, GameplayEvent, Unit]#Context, out: Collector[Unit]): Unit = {
      val profile = Option(profileState.value()).getOrElse(new PlayerProfile(event.player_id, event.run_id))

      val isFeatureTriggerEvent = event.event_type == "seer_encounter_begin" || event.event_type == "elite_fight_completed"
      if (isFeatureTriggerEvent) {
        val archetype = Try(event.payload.get("boss_archetype").toString.toLowerCase).getOrElse("none")
        if (archetype == "none") {
          logger.warn(s"Ignoring event '${event.event_type}' with 'none' archetype for run_id: ${ctx.getCurrentKey}")
          return // Stop processing this specific event
        }
      }

      event.event_type match {
          case "player_movement_event" =>
            val dirPayload = event.payload.get("dir").asInstanceOf[java.util.Map[String, Double]]
            profile.lastPlayerVelocity = PredictionVector(dirPayload.get("dx").toFloat, dirPayload.get("dy").toFloat)

          case "player_dash_event" =>
              profile.totalDashes += 1 // For Seer
              updateVexerPrediction(profile, event.payload, ctx) // For Vexer

          case "enemy_death_event" =>
              updateBruteAdaptation(profile, event.payload, ctx) // For Brute

          case "player_damage_dealt_event" =>
              val damage = Try(event.payload.get("dmg_amount").toString.toDouble).getOrElse(0.0)
              profile.totalDamageDealt += damage

          case "damage_taken_event" =>
              val damage = Try(event.payload.get("dmg_amount").toString.toDouble).getOrElse(0.0)
              profile.totalDamageTaken += damage
              if (Try(event.payload.get("is_elite_source").toString.toBoolean).getOrElse(false)) {
                  profile.damageTakenFromElites += damage
              }

          case "player_status_event" =>
              val hp = Try(event.payload.get("hp").toString.toDouble).getOrElse(1.0)
              val maxHp = Try(event.payload.get("max_hp").toString.toDouble).getOrElse(1.0)
              if (maxHp > 0) profile.hpHistory = (hp / maxHp :: profile.hpHistory).take(100)

          case "upgrade_choice" =>
              val id = Try(event.payload.get("chosen_upgrade_id").toString).getOrElse("unknown")
              profile.upgradeCounts = profile.upgradeCounts.updated(id, profile.upgradeCounts.getOrElse(id, 0) + 1)

          case "seer_encounter_begin" =>
              processFeatureVectorSnapshot(profile, event.payload, "boss", ctx)

          case "elite_fight_completed" =>
              processFeatureVectorSnapshot(profile, event.payload, "elite", ctx)

          case _ => // Ignore other events
      }
    profile.lastUpdated = System.currentTimeMillis()
    profileState.update(profile)
  }

    override def onTimer(ts: Long, ctx: KeyedProcessFunction[String, GameplayEvent, Unit]#OnTimerContext, out: Collector[Unit]): Unit = {
      val profile = profileState.value()
      if (profile != null && ts == profile.activeStaleTimer) {
        logger.info(s"Brute staleness timer fired for run_id: ${ctx.getCurrentKey}")
        val newForm = if (profile.currentBruteForm == "juggernaut") "skirmisher" else "juggernaut"
        profile.stalenessOverrideUntil = ctx.timerService().currentProcessingTime() + PlayerProfileAndAdaptiveParametersJob.stalenessGracePeriodMs
        sendBruteFormUpdate(newForm, profile, ctx)
      }
    }

    // --- Helper Methods for Each Feature ---

    private def updateVexerPrediction(profile: PlayerProfile, payload: java.util.Map[String, AnyRef], ctx: KeyedProcessFunction[String, GameplayEvent, Unit]#Context): Unit = {
        val dirPayload = payload.get("direction").asInstanceOf[java.util.Map[String, Double]]
        val dx = dirPayload.get("dx")
        val dy = dirPayload.get("dy")
        val direction = if (Math.abs(dx) > Math.abs(dy)) (if (dx > 0) "right" else "left") else (if (dy > 0) "up" else "down")

        val newCount = profile.dashDirectionCounts.getOrElse(direction, 0) + 1
        profile.dashDirectionCounts = profile.dashDirectionCounts.updated(direction, newCount)

        if (profile.dashDirectionCounts.nonEmpty) {
            val newPrediction = profile.dashDirectionCounts.maxBy(_._2)._1
            if (newPrediction != profile.lastVexerPrediction) {
                profile.lastVexerPrediction = newPrediction
                val vec = newPrediction match {
                    case "up" => PredictionVector(0, 1); case "down" => PredictionVector(0, -1)
                    case "left" => PredictionVector(-1, 0); case "right" => PredictionVector(1, 0)
                }
                val vexerPayload = VexerPredictionPayload(profile.playerId, vec)
                val envelope = AdaptiveMessageEnvelope("vexer_prediction_update", gson.toJson(vexerPayload))
                ctx.output(adaptiveTag, gson.toJson(envelope))
            }
        }
    }

    private def updateBruteAdaptation(profile: PlayerProfile, payload: java.util.Map[String, AnyRef], ctx: KeyedProcessFunction[String, GameplayEvent, Unit]#Context): Unit = {
      if (ctx.timerService().currentProcessingTime() < profile.stalenessOverrideUntil) return

      val enemyVelPayload = payload.get("velocity").asInstanceOf[java.util.Map[String, Double]]
      val (enemyVx, enemyVy) = (enemyVelPayload.get("vx"), enemyVelPayload.get("vy"))
      val (playerVx, playerVy) = (profile.lastPlayerVelocity.dx, profile.lastPlayerVelocity.dy)
      val enemyMag = sqrt(enemyVx * enemyVx + enemyVy * enemyVy)
      val playerMag = sqrt(playerVx * playerVx + playerVy * playerVy)

      if (enemyMag > 0 && playerMag > 0) {
        val dotProduct = ((playerVx / playerMag) * (enemyVx / enemyMag)) + ((playerVy / playerMag) * (enemyVy / enemyMag))
        profile.recentKillDotProducts = (dotProduct :: profile.recentKillDotProducts).take(PlayerProfileAndAdaptiveParametersJob.maxDotProductHistory)
      }

      var newForm = profile.currentBruteForm
      if (profile.recentKillDotProducts.size >= 5) {
        val avgDot = profile.recentKillDotProducts.sum / profile.recentKillDotProducts.size
        if (avgDot < -0.2) newForm = "juggernaut"
        else if (avgDot > 0.6) newForm = "skirmisher"
        else newForm = if (profile.currentBruteForm == "juggernaut") "skirmisher" else "juggernaut"
      }

      if (newForm != profile.currentBruteForm) {
        sendBruteFormUpdate(newForm, profile, ctx)
      }
    }

    private def sendBruteFormUpdate(newForm: String, profile: PlayerProfile, ctx: KeyedProcessFunction[String, GameplayEvent, Unit]#Context): Unit = {
      if (profile.activeStaleTimer != 0L) ctx.timerService().deleteProcessingTimeTimer(profile.activeStaleTimer)

      profile.currentBruteForm = newForm
      val payload = FormAdaptationPayload(profile.playerId, newForm)
      val envelope = AdaptiveMessageEnvelope("form_adaptation", gson.toJson(payload))
      ctx.output(adaptiveTag, gson.toJson(envelope))

      val newTimer = ctx.timerService().currentProcessingTime() + PlayerProfileAndAdaptiveParametersJob.stalenessCheckDurationMs
      ctx.timerService().registerProcessingTimeTimer(newTimer)
      profile.activeStaleTimer = newTimer
    }

    private def processFeatureVectorSnapshot(profile: PlayerProfile, eventPayload: java.util.Map[String, AnyRef], fightType: String, ctx: KeyedProcessFunction[String, GameplayEvent, Unit]#Context): Unit = {
      logger.info(s"Feature vector snapshot triggered by '$fightType' for run_id: ${ctx.getCurrentKey}. Processing...")

        val encounterId = Try(eventPayload.get("encounter_id").toString).getOrElse(java.util.UUID.randomUUID().toString)
        val avgHp = if (profile.hpHistory.isEmpty) 1.0 else profile.hpHistory.sum / profile.hpHistory.size

        val featureVector = FeatureVector(
            run_id = profile.runId,
            encounterId = encounterId,
            total_dashes = profile.totalDashes,
            total_damage_dealt = profile.totalDamageDealt,
            total_damage_taken = profile.totalDamageTaken,
            damage_taken_from_elites = profile.damageTakenFromElites,
            avg_hp_percent = avgHp,
            upgrade_counts = profile.upgradeCounts,
            outcome = -1, // Placeholder: Ground truth is set by the post-run Spark Job
            weight = 1.0  // Placeholder: Definitive weight is set by the post-run Spark Job
        )
        val featureJson = gson.toJson(featureVector)


        // 1. Emit to the 'bqml_features' topic via the Seer side output
        ctx.output(seerTag, featureJson)
        logger.info(s"Emitted feature vector to Seer topic for run_id: ${ctx.getCurrentKey}")

        // 2. Cache to HDFS, overwriting previous versions for this run
        val path = new org.apache.hadoop.fs.Path(s"$hdfsCachePath/run_id=${profile.runId}/features.json")
        Try {
            val outStream = hdfsFileSystem.create(path, true) // true = overwrite
            outStream.write(featureJson.getBytes(StandardCharsets.UTF_8))
            outStream.close()
            logger.info(s"Cached feature vector to HDFS: $path")
        } match {
            case Failure(e) => logger.error(s"Failed to write to HDFS cache at $path", e)
            case Success(_) => // Already logged
        }
    }

}