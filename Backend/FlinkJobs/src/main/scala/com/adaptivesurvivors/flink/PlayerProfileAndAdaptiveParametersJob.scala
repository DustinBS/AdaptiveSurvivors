// Backend/FlinkJobs/src/main/scala/com/adaptivesurvivors/flink/PlayerProfileAndAdaptiveParametersJob.scala

// This Flink job uses a KeyedProcessFunction to process real-time gameplay events
// from Kafka, maintain a player profile, and conditionally produce different,
// enveloped adaptive messages for different enemy types.

package com.adaptivesurvivors.flink

import com.google.gson.Gson
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

import java.time.Duration
import java.util.Properties
import com.google.gson._
import com.google.gson.reflect.TypeToken
import com.google.gson.stream.{JsonReader, JsonWriter}
import java.lang.reflect.ParameterizedType
import scala.math.sqrt

object SetTypeAdapterFactory extends TypeAdapterFactory {
  override def create[T](gson: Gson, tt: TypeToken[T]): TypeAdapter[T] = {
    val rawType = tt.getRawType
    if (!classOf[Set[_]].isAssignableFrom(rawType)) {
      return null
    }
    val elementType = tt.getType.asInstanceOf[ParameterizedType].getActualTypeArguments()(0)
    val elementTypeAdapter = gson.getAdapter(TypeToken.get(elementType)).asInstanceOf[TypeAdapter[Any]]
    new TypeAdapter[Set[_]] {
      override def write(out: JsonWriter, value: Set[_]): Unit = {
        if (value == null || value.isEmpty) {
          out.beginArray(); out.endArray()
        } else {
          out.beginArray(); value.foreach(elem => elementTypeAdapter.write(out, elem)); out.endArray()
        }
      }
      override def read(in: JsonReader): Set[_] = {
        in.beginArray()
        val set = scala.collection.mutable.Set.empty[Any]
        while (in.hasNext) { set += elementTypeAdapter.read(in) }
        in.endArray(); set.toSet
      }
    }.asInstanceOf[TypeAdapter[T]]
  }
}

// --- Data Models ---
case class GameplayEvent(event_type: String, timestamp: Long, player_id: String, payload: java.util.Map[String, AnyRef])

case class PlayerProfile(
  var playerId: String,
  // Vexer State
  var dashDirectionCounts: Map[String, Int] = Map.empty,
  var lastVexerPrediction: String = "none",
  // Brute State
  var lastPlayerVelocity: PredictionVector = PredictionVector(0, 0),
  var recentKillDotProducts: List[Double] = List.empty,
  var lastUpdated: Long = System.currentTimeMillis(),
  // --- State for Staleness Mechanic ---
  var currentBruteForm: String = "skirmisher", // Default form
  var activeStaleTimer: Long = 0L, // Timestamp of the currently set timer
  var stalenessOverrideUntil: Long = 0L // Timestamp for the grace period
)

// --- Envelope and Payload Models ---
case class AdaptiveMessageEnvelope(message_type: String, payload: String)
case class FormAdaptationPayload(playerId: String, adaptation_type: String)
case class VexerPredictionPayload(playerId: String, predicted_direction: PredictionVector)
case class PredictionVector(dx: Float, dy: Float)

class JsonToGameplayEventMapper extends RichMapFunction[String, GameplayEvent] {
  @transient private var gson: Gson = _
  override def open(parameters: Configuration): Unit = {
    gson = new GsonBuilder().registerTypeAdapterFactory(SetTypeAdapterFactory).create()
  }
  override def map(jsonString: String): GameplayEvent = {
    try {
      gson.fromJson(jsonString, classOf[GameplayEvent])
    } catch {
      case e: Exception =>
        println(s"Error parsing JSON: $jsonString - ${e.getMessage}")
        GameplayEvent("invalid_event", 0L, "unknown", new java.util.HashMap[String, AnyRef]())
    }
  }
}

object PlayerProfileAndAdaptiveParametersJob {
  val maxDotProductHistory = 20
  val stalenessCheckDurationMs = 5000 // 5 seconds
  val stalenessGracePeriodMs = 5000 // 5 seconds

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    val gameplayEventsSource = KafkaSource.builder[String]()
      .setBootstrapServers(kafkaBootstrapServers)
      .setTopics("gameplay_events")
      .setGroupId("flink-gameplay-events-consumer")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val rawJsonStream: DataStream[String] = env.fromSource(gameplayEventsSource, WatermarkStrategy.noWatermarks(), "Kafka Gameplay Events Source")
    val gameplayEventStream: DataStream[GameplayEvent] = rawJsonStream.map(new JsonToGameplayEventMapper()).filter(_.event_type != "invalid_event")

    val keyedEvents = gameplayEventStream.keyBy(_.player_id)

    val envelopedJsonStream: DataStream[String] = keyedEvents.process(new PlayerProfileUpdater())

    val adaptiveParamsSink = KafkaSink.builder[String]()
      .setBootstrapServers(kafkaBootstrapServers)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("adaptive_params")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()

    envelopedJsonStream.sinkTo(adaptiveParamsSink).name("Kafka Adaptive Parameters Sink")
    env.execute("Adaptive Survivors Player Profile and Adaptive Parameters Job")
  }

  class PlayerProfileUpdater extends KeyedProcessFunction[String, GameplayEvent, String] {
    private var playerProfileState: ValueState[PlayerProfile] = _
    @transient private var gson: Gson = _

    override def open(parameters: Configuration): Unit = {
      val descriptor = new ValueStateDescriptor("player-profile", createTypeInformation[PlayerProfile])
      playerProfileState = getRuntimeContext.getState(descriptor)
      gson = new GsonBuilder().registerTypeAdapterFactory(SetTypeAdapterFactory).create()
    }

    override def processElement(event: GameplayEvent, ctx: KeyedProcessFunction[String, GameplayEvent, String]#Context, out: Collector[String]): Unit = {
      val currentProfile = Option(playerProfileState.value()).getOrElse(PlayerProfile(event.player_id))
      val currentTime = ctx.timerService().currentProcessingTime()

      // --- Event Processing Logic ---
      event.event_type match {
        case "player_movement_event" =>
          val dirPayload = event.payload.get("dir").asInstanceOf[java.util.Map[String, Double]]
          currentProfile.lastPlayerVelocity = PredictionVector(dirPayload.get("dx").toFloat, dirPayload.get("dy").toFloat)

        case "enemy_death_event" =>
          // --- Grace Period Check ---
          // If we are inside a staleness override grace period, do NOT run the normal logic.
          if (currentTime < currentProfile.stalenessOverrideUntil) {
            // Do nothing, let the stale form persist for the grace period
          } else {
            // It's safe to run the normal dot product logic
            val enemyVelPayload = event.payload.get("velocity").asInstanceOf[java.util.Map[String, Double]]
            val enemyVel = (enemyVelPayload.get("vx"), enemyVelPayload.get("vy"))
            val playerVel = (currentProfile.lastPlayerVelocity.dx, currentProfile.lastPlayerVelocity.dy)
            val enemyMag = sqrt(enemyVel._1 * enemyVel._1 + enemyVel._2 * enemyVel._2)
            val playerMag = sqrt(playerVel._1 * playerVel._1 + playerVel._2 * playerVel._2)

            if (enemyMag > 0 && playerMag > 0) {
              val normEnemyVel = (enemyVel._1 / enemyMag, enemyVel._2 / enemyMag)
              val normPlayerVel = (playerVel._1 / playerMag, playerVel._2 / playerMag)
              val dotProduct = (normPlayerVel._1 * normEnemyVel._1) + (normPlayerVel._2 * normEnemyVel._2)
              currentProfile.recentKillDotProducts = (dotProduct :: currentProfile.recentKillDotProducts).take(maxDotProductHistory)
            }

            // --- Tuned Adaptation Logic ---
            var newAdaptationType = currentProfile.currentBruteForm
            if (currentProfile.recentKillDotProducts.size >= 5) {
              val avgDotProduct = currentProfile.recentKillDotProducts.sum / currentProfile.recentKillDotProducts.size

              if (avgDotProduct < -0.2) { // Wider window for Juggernaut
                newAdaptationType = "juggernaut"
              } else if (avgDotProduct > 0.6) { // Narrower window for Skirmisher
                newAdaptationType = "skirmisher"
              } else {
                // Default to the opposite form to encourage variety
                newAdaptationType = if(currentProfile.currentBruteForm == "juggernaut") "skirmisher" else "juggernaut"
              }
            }

            // If the form changed, send a message and reset the staleness timer
            if (newAdaptationType != currentProfile.currentBruteForm) {
              sendBruteFormUpdate(newAdaptationType, currentProfile, ctx, out)
            }
          }

        case "player_dash_event" =>
          // This logic for the Vexer remains separate and unchanged.
          val dirPayload = event.payload.get("direction").asInstanceOf[java.util.Map[String, Double]]
          val dx = dirPayload.get("dx")
          val dy = dirPayload.get("dy")
          val direction = if (Math.abs(dx) > Math.abs(dy)) (if (dx > 0) "right" else "left") else (if (dy > 0) "up" else "down")
          val newCount = currentProfile.dashDirectionCounts.getOrElse(direction, 0) + 1
          currentProfile.dashDirectionCounts = currentProfile.dashDirectionCounts.updated(direction, newCount)

          if (currentProfile.dashDirectionCounts.nonEmpty) {
            val newPrediction = currentProfile.dashDirectionCounts.maxBy(_._2)._1
            if (newPrediction != currentProfile.lastVexerPrediction) {
              currentProfile.lastVexerPrediction = newPrediction
              val directionVector = newPrediction match {
                case "up" => PredictionVector(0, 1); case "down" => PredictionVector(0, -1)
                case "left" => PredictionVector(-1, 0); case "right" => PredictionVector(1, 0)
              }
              val vexerPayload = VexerPredictionPayload(currentProfile.playerId, directionVector)
              val vexerEnvelope = AdaptiveMessageEnvelope("vexer_prediction_update", gson.toJson(vexerPayload))
              out.collect(gson.toJson(vexerEnvelope))
            }
          }

        case _ => // Ignore other events for this particular job logic
      }

      currentProfile.lastUpdated = System.currentTimeMillis()
      playerProfileState.update(currentProfile)
    }
    /**
     * This method is called by Flink when a registered timer fires.
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, GameplayEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      val currentProfile = playerProfileState.value()

      // Only act if the timer that fired is the one we have stored (prevents acting on old, orphaned timers)
      if (currentProfile != null && timestamp == currentProfile.activeStaleTimer) {
        // Force a switch to the opposite form
        val newForm = if (currentProfile.currentBruteForm == "juggernaut") "skirmisher" else "juggernaut"

        // --- Set the Grace Period ---
        currentProfile.stalenessOverrideUntil = ctx.timerService().currentProcessingTime() + stalenessGracePeriodMs

        // Send the update and update the state
        sendBruteFormUpdate(newForm, currentProfile, ctx, out)
      }
    }

    /**
     * Helper function to send Brute form updates and manage timers.
     */
    private def sendBruteFormUpdate(newForm: String, profile: PlayerProfile, ctx: KeyedProcessFunction[String, GameplayEvent, String]#Context, out: Collector[String]): Unit = {
        // Delete any previously existing timer
        if (profile.activeStaleTimer != 0L) {
          ctx.timerService().deleteProcessingTimeTimer(profile.activeStaleTimer)
        }

        // Update state
        profile.currentBruteForm = newForm

        // Send the message
        val payload = FormAdaptationPayload(profile.playerId, newForm)
        val envelope = AdaptiveMessageEnvelope("form_adaptation", gson.toJson(payload))
        out.collect(gson.toJson(envelope))

        // Register a new timer for the new form
        val newTimerTimestamp = ctx.timerService().currentProcessingTime() + stalenessCheckDurationMs
        ctx.timerService().registerProcessingTimeTimer(newTimerTimestamp)
        profile.activeStaleTimer = newTimerTimestamp

        playerProfileState.update(profile)
    }
  }
}
