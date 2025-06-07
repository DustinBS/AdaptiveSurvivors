// Backend/FlinkJobs/src/main/scala/com/adaptivesurvivors/flink/PlayerProfileAndAdaptiveParametersJob.scala

// This Flink job processes real-time gameplay events from Kafka,
// maintains a player profile, derives adaptive parameters,
// and publishes them back to Kafka.

package com.adaptivesurvivors.flink

import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.{WatermarkStrategy, TimestampAssignerSupplier}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.{KafkaSink, KafkaRecordSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util.Properties

import com.google.gson._
import com.google.gson.reflect.TypeToken
import com.google.gson.stream.{JsonReader, JsonWriter}
import java.lang.reflect.ParameterizedType

/**
 * A custom Gson TypeAdapterFactory to handle Scala Sets correctly.
 * It ensures that a null or empty Set is always serialized as an empty JSON array `[]`,
 * which is compatible with C#/.NET deserializers like Newtonsoft.Json.
 */
object SetTypeAdapterFactory extends TypeAdapterFactory {
  override def create[T](gson: Gson, tt: TypeToken[T]): TypeAdapter[T] = {
    val rawType = tt.getRawType
    if (!classOf[Set[_]].isAssignableFrom(rawType)) {
      return null // This factory only handles Sets
    }

    val elementType = tt.getType.asInstanceOf[ParameterizedType].getActualTypeArguments()(0)
    val elementTypeAdapter = gson.getAdapter(TypeToken.get(elementType)).asInstanceOf[TypeAdapter[Any]]

    new TypeAdapter[Set[_]] {
      override def write(out: JsonWriter, value: Set[_]): Unit = {
        if (value == null || value.isEmpty) {
          out.beginArray() // Write an empty array for null or empty sets
          out.endArray()
        } else {
          out.beginArray()
          value.foreach(elem => elementTypeAdapter.write(out, elem))
          out.endArray()
        }
      }

      override def read(in: JsonReader): Set[_] = {
        // We only need this for serialization, so reading can be left unimplemented or basic.
        // This job does not read JSON into a Set, so we don't need a complex implementation.
        in.beginArray()
        val set = scala.collection.mutable.Set.empty[Any]
        while (in.hasNext) {
          set += elementTypeAdapter.read(in)
        }
        in.endArray()
        set.toSet
      }
    }.asInstanceOf[TypeAdapter[T]]
  }
}

// --- Data Models ---
// These case classes represent the structure of our JSON events and state.

/**
 * Represents an incoming gameplay event from the Unity client.
 *
 * @param event_type The type of the event (e.g., "player_movement_event", "weapon_hit_event").
 * @param timestamp  The timestamp of the event.
 * @param player_id  The ID of the player associated with the event.
 * @param payload    A generic map for event-specific data.
 */
case class GameplayEvent(
  event_type: String,
  timestamp: Long, // Unix timestamp in milliseconds
  player_id: String,
  payload: java.util.Map[String, AnyRef] // Using AnyRef for flexibility with JSON parsing
)

/**
 * Represents the player's real-time profile, updated continuously.
 * This is a simplified version and can be expanded with more metrics.
 *
 * @param playerId The ID of the player.
 * @param totalDamageDealt Total damage dealt by the player.
 * @param totalDamageTaken Total damage taken by the player.
 * @param weaponsUsed A map storing the count of hits for each weapon ID.
 * @param commonDodgeVector A simplified representation of player's dodge direction (e.g., "left", "right").
 * @param playstyleTags Tags describing the player's playstyle (e.g., "Aggressive", "Kiting").
 * @param lastUpdated Timestamp of the last update to this profile.
 */
case class PlayerProfile(
  var playerId: String,
  var totalDamageDealt: Double = 0.0,
  var totalDamageTaken: Double = 0.0,
  var weaponsUsed: Map[String, Int] = Map.empty,
  var commonDodgeVector: String = "none", // Example: "left", "right", "forward", "backward"
  var playstyleTags: Set[String] = Set.empty, // Example: "Aggressive", "Defensive", "Kiting"
  var lastUpdated: Long = System.currentTimeMillis()
)

/**
 * Represents the adaptive parameters generated for Elite enemies and breakable objects.
 * These are sent back to the game client.
 *
 * @param playerId The ID of the player for whom these parameters are generated.
 * @param enemyResistances A map of weapon ID to resistance percentage (0.0 to 1.0).
 * @param eliteBehaviorShift A string indicating a behavioral shift (e.g., "speed_boost", "slow_on_hit").
 * @param eliteStatusImmunities A set of status effects to which Elites are temporarily immune.
 * @param breakableObjectBuffsDebuffs A map of object type to buff/debuff (e.g., "Tombstone" -> "-5%_move_speed").
 * @param timestamp The timestamp when these parameters were generated.
 */
case class AdaptiveParameters(
  playerId: String,
  enemyResistances: Map[String, Double] = Map.empty,
  eliteBehaviorShift: String = "none",
  eliteStatusImmunities: Set[String] = Set.empty,
  breakableObjectBuffsDebuffs: Map[String, String] = Map.empty,
  timestamp: Long = System.currentTimeMillis()
)

/**
 * A RichMapFunction to parse JSON strings into GameplayEvent objects.
 * By initializing the Gson instance in the open() method, we ensure that the
 * non-serializable Gson object is created on the TaskManager, not the JobManager,
 * thus avoiding "Task not serializable" errors.
 */
class JsonToGameplayEventMapper extends RichMapFunction[String, GameplayEvent] {
  // Declare a transient, lazy Gson instance.
  // 'transient' tells the serializer to ignore it.
  // We will initialize it properly in the open() method.
  @transient private var gson: Gson = _

  override def open(parameters: Configuration): Unit = {
    // This method is called once per task on the worker node.
    // It's the perfect place to initialize non-serializable objects.
    gson = new GsonBuilder()
      .registerTypeAdapterFactory(SetTypeAdapterFactory)
      .create()
  }

  override def map(jsonString: String): GameplayEvent = {
    try {
      gson.fromJson(jsonString, classOf[GameplayEvent])
    } catch {
      case e: Exception =>
        println(s"Error parsing JSON: $jsonString - ${e.getMessage}")
        // Return a dummy event to be filtered out later
        GameplayEvent("invalid_event", 0L, "unknown", new java.util.HashMap[String, AnyRef]())
    }
  }
}

/**
 * The main Flink job entry point.
 */
object PlayerProfileAndAdaptiveParametersJob {

  def main(args: Array[String]): Unit = {
    // 1. Setup the Streaming Execution Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Kafka broker addresses from environment variables or default
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092") // Use localhost:29092 for external access from host, or kafka:9092 for internal Docker communication

    // Kafka consumer properties
    val consumerProps = new Properties()
    consumerProps.setProperty("bootstrap.servers", kafkaBootstrapServers)
    consumerProps.setProperty("group.id", "flink-adaptive-survivors-group")
    consumerProps.setProperty("auto.offset.reset", "latest") // Start consuming from the latest offset

    // Kafka producer properties
    val producerProps = new Properties()
    producerProps.setProperty("bootstrap.servers", kafkaBootstrapServers)

    // 2. Configure Kafka Source for gameplay_events
    val gameplayEventsSource = KafkaSource.builder[String]()
      .setBootstrapServers(kafkaBootstrapServers)
      .setTopics("gameplay_events")
      .setGroupId("flink-gameplay-events-consumer")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    // 3. Create a DataStream of raw JSON strings from Kafka
    val rawJsonStream: DataStream[String] = env.fromSource(
      gameplayEventsSource,
      WatermarkStrategy.noWatermarks(),
      "Kafka Gameplay Events Source"
    )

    // 4. Parse the JSON strings using the new RichMapFunction
    val gameplayEventStream: DataStream[GameplayEvent] = rawJsonStream
      .map(new JsonToGameplayEventMapper())
      .filter(_.event_type != "invalid_event")

    // 5. Assign Timestamps and Watermarks to the typed stream
    val timedEventStream = gameplayEventStream
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[GameplayEvent](Duration.ofSeconds(5)) // Allow 5s of lateness
          .withTimestampAssigner(
            new org.apache.flink.api.common.eventtime.SerializableTimestampAssigner[GameplayEvent] {
              // Extract the timestamp from the event object itself
              override def extractTimestamp(element: GameplayEvent, recordTimestamp: Long): Long = element.timestamp
            }
          )
      )

    // 6. Key the stream by player_id for stateful processing
    val keyedEvents = timedEventStream.keyBy(_.player_id)

    // 7. Apply the PlayerProfileUpdater to update profile and generate adaptive parameters
    val adaptiveParamsStream: DataStream[AdaptiveParameters] = keyedEvents
      .map(new PlayerProfileAndAdaptiveParametersUpdater())

    // 8. Configure Kafka Sink for adaptive_params
    val adaptiveParamsSink = KafkaSink.builder[String]()
      .setBootstrapServers(kafkaBootstrapServers)
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("adaptive_params")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .build()

    // 9. Write the adaptive parameters stream to Kafka
    adaptiveParamsStream
      .map(params => {
          // use another RichMapFunction if performance is critical.
          val gson = new GsonBuilder().registerTypeAdapterFactory(SetTypeAdapterFactory).create()
          gson.toJson(params)
      })
      .sinkTo(adaptiveParamsSink)
      .name("Kafka Adaptive Parameters Sink")

    // 10. Execute the Flink job
    env.execute("Adaptive Survivors Player Profile and Adaptive Parameters Job")
  }

  /**
   * Flink RichMapFunction to maintain player profile state and generate adaptive parameters.
   * This function will be called for each event, keyed by player_id.
   */
  class PlayerProfileAndAdaptiveParametersUpdater extends RichMapFunction[GameplayEvent, AdaptiveParameters] {

    // ValueState to hold the PlayerProfile for each player_id
    private var playerProfileState: ValueState[PlayerProfile] = _

    override def open(parameters: Configuration): Unit = {
      // Initialize the ValueStateDescriptor for PlayerProfile
      val descriptor = new ValueStateDescriptor(
        "player-profile", // The state name
        createTypeInformation[PlayerProfile] // Type information for the state
      )
      playerProfileState = getRuntimeContext.getState(descriptor)
    }

    override def map(event: GameplayEvent): AdaptiveParameters = {
      // Get current player profile or create a new one if it doesn't exist
      val currentProfile = Option(playerProfileState.value()) match {
        case Some(profile) => profile
        case None          => PlayerProfile(event.player_id)
      }

      // --- Update Player Profile based on the event ---
      event.event_type match {
        case "player_movement_event" =>
          // Safely extract the nested 'dir' map
          val dirPayload = event.payload.get("dir") match {
            case m: java.util.Map[_, _] => m.asInstanceOf[java.util.Map[String, AnyRef]]
            case _                      => new java.util.HashMap[String, AnyRef]() // Default to an empty map
          }

          // Safely extract dx and dy, handling different numeric types from JSON
          val dirX = dirPayload.get("dx") match {
            case d: java.lang.Double  => d.doubleValue()
            case i: java.lang.Integer => i.doubleValue()
            case _                    => 0.0
          }

          val dirY = dirPayload.get("dy") match {
            case d: java.lang.Double  => d.doubleValue()
            case i: java.lang.Integer => i.doubleValue()
            case _                    => 0.0
          }

          if (dirX > 0.5) currentProfile.commonDodgeVector = "right"
          else if (dirX < -0.5) currentProfile.commonDodgeVector = "left"
          else if (dirY > 0.5) currentProfile.commonDodgeVector = "forward"
          else if (dirY < -0.5) currentProfile.commonDodgeVector = "backward"
          else currentProfile.commonDodgeVector = "none"

          // Example of playstyle tag based on movement
          if (dirX != 0.0 || dirY != 0.0) { // If player is actively moving
            currentProfile.playstyleTags += "Active"
          } else {
            currentProfile.playstyleTags -= "Active"
          }

        case "weapon_hit_event" =>
          val dmgDealt = event.payload.get("dmg_dealt") match {
            case d: java.lang.Double  => d.doubleValue()
            case i: java.lang.Integer => i.doubleValue()
            case _                    => 0.0
          }
          val weaponId = event.payload.get("weapon_id") match {
            case s: String => s
            case _         => "unknown"
          }
          currentProfile.totalDamageDealt += dmgDealt
          currentProfile.weaponsUsed = currentProfile.weaponsUsed.updated(weaponId, currentProfile.weaponsUsed.getOrElse(weaponId, 0) + 1)

          if (currentProfile.totalDamageDealt > 1000) {
            currentProfile.playstyleTags += "Aggressive"
            currentProfile.playstyleTags -= "Kiting"
          }

        case "damage_taken_event" =>
          val dmgAmount = event.payload.get("dmg_amount") match {
            case d: java.lang.Double  => d.doubleValue()
            case i: java.lang.Integer => i.doubleValue()
            case _                    => 0.0
          }
          currentProfile.totalDamageTaken += dmgAmount

          if (currentProfile.totalDamageTaken > 500) {
            currentProfile.playstyleTags += "Reckless"
          }

        case "upgrade_choice_event" =>
          val chosenUpgradeId = event.payload.get("chosen_upgrade_id") match {
            case s: String => s
            case _         => "none"
          }
          if (chosenUpgradeId.contains("speed")) currentProfile.playstyleTags += "Kiting"
          if (chosenUpgradeId.contains("armor")) currentProfile.playstyleTags += "Defensive"

        case "enemy_death_event" =>
          val killedByWeaponId = event.payload.get("killed_by_weapon_id") match {
            case s: String => s
            case _         => "unknown"
          }
          currentProfile.weaponsUsed = currentProfile.weaponsUsed.updated(killedByWeaponId, currentProfile.weaponsUsed.getOrElse(killedByWeaponId, 0) + 1)

        case "breakable_object_destroyed_event" =>
          val objType = event.payload.get("obj_type") match {
            case s: String => s
            case _         => "unknown"
          }
          // Your logic for object destruction counts would go here

        case _ => // Handle other event types or ignore
      }

      currentProfile.lastUpdated = System.currentTimeMillis()
      playerProfileState.update(currentProfile)

      // --- Generate Adaptive Parameters based on the updated Player Profile ---
      var enemyResistances: Map[String, Double] = Map.empty
      var eliteBehaviorShift: String = "none"
      var eliteStatusImmunities: Set[String] = Set.empty
      var breakableObjectBuffsDebuffs: Map[String, String] = Map.empty

      if (currentProfile.weaponsUsed.nonEmpty) {
        val topWeapons = currentProfile.weaponsUsed.toSeq.sortBy(-_._2).take(3).map(_._1)
        topWeapons.foreach(weaponId => {
          enemyResistances = enemyResistances.updated(weaponId, 0.25)
        })
      }

      currentProfile.commonDodgeVector match {
        case "left" | "right" | "forward" | "backward" =>
          eliteBehaviorShift = "anticipate_" + currentProfile.commonDodgeVector
        case _ =>
      }

      if (currentProfile.playstyleTags.contains("Aggressive")) {
        eliteBehaviorShift = "speed_aggression_boost"
      } else if (currentProfile.playstyleTags.contains("Kiting")) {
        eliteBehaviorShift = "temporary_slow_on_hit"
      }

      if (currentProfile.playstyleTags.contains("Aggressive") && currentProfile.totalDamageDealt > 2000) {
        eliteStatusImmunities += "freeze"
      }

      // Ensure empty collections are handled correctly for consistent JSON serialization.
      // An empty Scala Set/Map can sometimes serialize to "{}" instead of "[]",
      // which causes issues with C#/.NET deserializers that expect an array.
      AdaptiveParameters(
        playerId = currentProfile.playerId,
        enemyResistances = if (enemyResistances.isEmpty) Map.empty else enemyResistances,
        eliteBehaviorShift = eliteBehaviorShift,
        eliteStatusImmunities = if (eliteStatusImmunities.isEmpty) Set.empty else eliteStatusImmunities,
        breakableObjectBuffsDebuffs = if (breakableObjectBuffsDebuffs.isEmpty) Map.empty else breakableObjectBuffsDebuffs
      )
    }
  }
}
