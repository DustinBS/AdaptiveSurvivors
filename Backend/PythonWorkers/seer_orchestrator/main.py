import os
import json
import logging
import random
import threading
import math
import yaml
import time
from typing import Dict, Any, List
import re

from hdfs import InsecureClient
from flask import Flask, jsonify
from kafka import KafkaProducer, KafkaConsumer
import google.generativeai as genai
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# --- Initial Setup & Configuration ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Use a lock to ensure thread-safe config reloads and access to global config
config_lock = threading.Lock()

# --- Global Variables (will be managed by reload function) ---
# These variables store the application's configuration and initialized clients.
# They are declared globally to allow re-initialization upon config changes.
CONFIG: Dict[str, Any] = {}
APP_SETTINGS: Dict[str, Any] = {}
producer: KafkaProducer | None = None
bq_client: bigquery.Client | None = None
llm_model: genai.GenerativeModel | None = None

# Flags to track the availability of external GCP services.
# These allow the worker to run in an "offline" mode using fallbacks.
gcp_bq_available: bool = False
gcp_llm_available: bool = False


# --- Environment Variable & Client Initialization ---

def get_required_env_var(key: str) -> str:
    """Gets an environment variable or exits if not found.
    Used for essential variables like Kafka brokers that are required for the service to run at all.
    """
    value = os.environ.get(key)
    if not value:
        logging.error(f"FATAL: Required environment variable '{key}' is not set.")
        exit(1) # Exit if essential environment variables are missing
    return value

# --- Flask App ---
app = Flask(__name__)

# --- Configuration Loading & Client Initialization ---
def load_and_reinitialize(initial_load: bool = False) -> tuple[bool, str]:
    """Loads the YAML configuration file and re-initializes all clients.
    This function is designed to be called at startup and via the /reload-config endpoint.
    It gracefully handles missing credentials for GCP services, allowing the worker to
    run in an offline mode.
    """
    global CONFIG, APP_SETTINGS, producer, llm_model, bq_client, gcp_bq_available, gcp_llm_available

    with config_lock:
        try:
            with open("config.yml", "r") as f:
                new_config = yaml.safe_load(f)
            logging.info("Configuration file 'config.yml' loaded successfully.")
        except (FileNotFoundError, yaml.YAMLError) as e:
            error_msg = f"FATAL: Could not load 'config.yml': {e}"
            logging.error(error_msg)
            if initial_load:
                exit(1) # Config file is essential, so exit on initial load failure
            return False, error_msg

        # Update global configuration
        CONFIG = new_config
        APP_SETTINGS = CONFIG.get('app_settings', {})

        # --- Initialize Kafka Producer (Mandatory) ---
        try:
            kafka_brokers = get_required_env_var("KAFKA_BROKERS")
            if producer:
                producer.close()
            producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                bootstrap_servers=kafka_brokers,
                retries=APP_SETTINGS.get("kafka_producer_retries", 5)
            )
            logging.info("Kafka Producer initialized successfully.")
        except Exception as e:
            error_msg = f"FATAL: Failed to initialize Kafka Producer: {e}"
            logging.error(error_msg)
            if initial_load:
                exit(1) # Kafka is essential, so exit on failure
            return False, error_msg

        # --- Initialize BigQuery Client (Optional) ---
        gcp_project_id = os.environ.get("GCP_PROJECT_ID")
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if gcp_project_id and credentials_path and os.path.exists(credentials_path):
            try:
                bq_client = bigquery.Client(project=gcp_project_id)
                bq_client.list_datasets(max_results=1) # Test authentication
                gcp_bq_available = True
                logging.info("BigQuery client initialized and authenticated successfully. BQML pipeline is ENABLED.")
            except Exception as e:
                gcp_bq_available = False
                logging.warning(f"Could not initialize BigQuery client: {e}. BQML pipeline is DISABLED.")
        else:
            gcp_bq_available = False
            logging.warning("GCP Project ID or credentials file not found. BQML pipeline is DISABLED.")

        # --- Initialize Generative AI Model (Optional) ---
        gemini_api_key = os.environ.get("GEMINI_API_KEY")
        if gemini_api_key:
            try:
                genai.configure(api_key=gemini_api_key)
                llm_model = genai.GenerativeModel(APP_SETTINGS.get("gemini_model", "gemini-1.5-flash"))
                gcp_llm_available = True
                logging.info("Gemini Model initialized successfully. LLM dialogue is ENABLED.")
            except Exception as e:
                gcp_llm_available = False
                logging.warning(f"Could not initialize Gemini Model: {e}. LLM dialogue is DISABLED.")
        else:
            gcp_llm_available = False
            logging.warning("GEMINI_API_KEY not found. LLM dialogue is DISABLED.")

    return True, "Configuration reloaded and clients re-initialized."

# --- Helper Functions ---

def scale_modifier(base_modifier: float, confidence: float) -> float:
    """Scales a bargain's power based on the prediction confidence."""
    with config_lock:
        scaling_factor = CONFIG.get('confidence_scaling_factor', 1.5)
    if confidence < 0.5:
        confidence = 0.5
    normalized_confidence = (confidence - 0.5) * 2
    scaled_effect = base_modifier * (1 + (scaling_factor * math.pow(normalized_confidence, 2)))
    return round(scaled_effect, 3)

def generate_fallback_response() -> tuple[str, list]:
    """Creates a generic fallback response when the main pipeline (BQML/LLM) fails."""
    logging.warning("Generating a fallback seer response.")
    dialogue = "The visions are... unclear. The threads of fate refuse to be read. I can offer you a choice, but its nature is as murky as the future itself."
    with config_lock:
        fallback_options = CONFIG.get('fallback_choices', {})
    choices = random.sample(fallback_options.get('buffs', []), 2) if random.choice([True, False]) else \
              random.sample(fallback_options.get('debuffs', []), 2)
    return dialogue, choices

def run_bqml_pipeline(features: dict, boss_archetype: str) -> Dict[str, Any] | None:
    """
    Executes the BQML pipeline by training a model, then uses ML.WEIGHTS to determine
    the most impactful features for the seer's response.
    """
    if not gcp_bq_available or not bq_client:
        logging.warning("BQML pipeline is disabled. Skipping.")
        return None

    # --- The retry logic for table creation is still valuable ---
    max_retries = 3
    retry_delay_seconds = 15
    for attempt in range(max_retries):
        try:
            with config_lock:
                bigquery_dataset = APP_SETTINGS.get("bigquery_default_dataset", "seer_training_workspace")
            table_name = f"{boss_archetype}_training_data"
            model_name = f"{boss_archetype}_seer_model"
            model_uri = f"{bq_client.project}.{bigquery_dataset}.{model_name}"
            table_uri = f"{bq_client.project}.{bigquery_dataset}.{table_name}"

            # --- Step 1: Train the model with the mandatory TRANSFORM clause for feature scaling ---
            sql_train = f"""
                CREATE OR REPLACE MODEL `{model_uri}`
                TRANSFORM(
                    ML.STANDARD_SCALER(total_dashes) OVER() AS total_dashes,
                    ML.STANDARD_SCALER(total_damage_dealt) OVER() AS total_damage_dealt,
                    ML.STANDARD_SCALER(total_damage_taken) OVER() AS total_damage_taken,
                    ML.STANDARD_SCALER(damage_taken_from_elites) OVER() AS damage_taken_from_elites,
                    ML.STANDARD_SCALER(avg_hp_percent) OVER() AS avg_hp_percent,
                    outcome
                )
                OPTIONS(
                    model_type='LOGISTIC_REG',
                    input_label_cols=['outcome'],
                    max_iterations=10,
                    early_stop=TRUE,
                    min_rel_progress=0.01
                ) AS
                SELECT * EXCEPT(upgrade_counts)
                FROM `{table_uri}`
                WHERE outcome IS NOT NULL;
            """
            bq_client.query(sql_train).result()
            logging.info(f"Successfully trained scaled BQML model: {model_name}")

            # --- Step 2: Query the model's weights to find the most important features ---
            sql_weights = f"SELECT processed_input, weight FROM ML.WEIGHTS(MODEL `{model_uri}`) WHERE processed_input != 'intercept'"
            weights_results = list(bq_client.query(sql_weights).result())

            if not weights_results:
                logging.error("Could not retrieve model weights.")
                return None

            # Find the top two features by the absolute magnitude of their learned weight
            sorted_weights = sorted(weights_results, key=lambda x: abs(x.weight), reverse=True)

            top_features = [row.processed_input for row in sorted_weights[:2]]
            if len(top_features) < 2:
                top_features.extend(['default'] * (2 - len(top_features)))

            # --- Step 3: Get the predicted outcome AND the probability (confidence) ---
            feature_sql_parts = []
            for k, v in features.items():
                 if k in ['total_dashes', 'total_damage_dealt', 'total_damage_taken', 'damage_taken_from_elites', 'avg_hp_percent']:
                    feature_sql_parts.append(f"{v} AS {k}")

            sql_predict_outcome = f"SELECT predicted_outcome, predicted_outcome_probs FROM ML.PREDICT(MODEL `{model_uri}`, (SELECT {', '.join(feature_sql_parts)}))"
            prediction_result = list(bq_client.query(sql_predict_outcome).result())

            if not prediction_result:
                logging.error("Prediction query returned no results.")
                return None

            prediction_row = prediction_result[0]
            predicted_outcome = prediction_row.predicted_outcome

            confidence_score = 0.5  # Default confidence
            for prob_struct in prediction_row.predicted_outcome_probs:
                if prob_struct['label'] == predicted_outcome:
                    confidence_score = prob_struct['prob']
                    break

            return {
                "prediction": "Win" if predicted_outcome == 1 else "Loss",
                "confidence": confidence_score,
                "primary_feature": top_features[0],
                "secondary_feature": top_features[1],
            }

        except NotFound:
            logging.warning(f"BQML table not found (Attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay_seconds}s...")
            time.sleep(retry_delay_seconds)
            retry_delay_seconds *= 2

        except Exception as e:
            logging.error(f"An unexpected BQML error occurred: {e}", exc_info=True)
            return None

    logging.error("BQML pipeline failed after multiple retries.")
    return None

# --- Core Logic ---

def process_seer_pipeline(trigger_payload: dict):
    """Orchestrates the seer encounter logic, now triggered by a lightweight message."""
    run_id = trigger_payload.get("run_id")
    encounter_id = trigger_payload.get("encounter_id")
    boss_archetype = trigger_payload.get("boss_archetype", "melee") # Use archetype from trigger

    if not run_id or not encounter_id:
        logging.error(f"Trigger message missing run_id or encounterId. Payload: {trigger_payload}")
        return

    logging.info(f"Processing pipeline for run_id: {run_id}, encounterId: {encounter_id}")

    # --- Step 1: Pull Feature Vector from HDFS ---
    try:
        # NOTE: Assumes HDFS is accessible at 'namenode:9870' from the Docker network.
        # This matches the port exposed in your docker-compose.yml for the namenode UI.
        hdfs_client = InsecureClient('http://namenode:9870', user='root')
        hdfs_path = f'/feature_store/live/run_id={run_id}/features.json'

        with hdfs_client.read(hdfs_path) as reader:
            features = json.load(reader)
        logging.info(f"Successfully pulled feature vector {features} from HDFS for run_id: {run_id}")

    except Exception as e:
        logging.error(f"Failed to pull feature vector from HDFS for run_id {run_id}: {e}. Aborting pipeline.")
        # Optionally, send a fallback response to Kafka here.
        return


    dialogue, choices = generate_fallback_response()

    pipeline_result = run_bqml_pipeline(features, boss_archetype=boss_archetype)

    if pipeline_result and gcp_llm_available and llm_model:
        is_buff = pipeline_result["prediction"] == "Loss"
        prompt_type = "loss" if is_buff else "win"

        with config_lock:
            prompt_template = CONFIG['prompts'][prompt_type]
            bargain_map = CONFIG['feature_to_bargain_map']

        prompt = prompt_template.format(
            primary_feature=pipeline_result["primary_feature"].replace("_", " "),
            secondary_feature=pipeline_result["secondary_feature"].replace("_", " ")
        )
        try:
            response_text = llm_model.generate_content(prompt).text

            logging.info(f"DEBUG: Raw response from Gemini API:\n---\n{response_text}\n---")

            # Search for a JSON object within the response text
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)

            if not json_match:
                raise ValueError("No JSON object found in the Gemini API response.")

            # Extract and parse only the matched JSON string
            json_string = json_match.group(0)
            llm_json = json.loads(json_string)

            dialogue = llm_json.get("dialogue", "The vision is complete.")
            descriptions = llm_json.get("bargain_descriptions", ["A choice.", "Another choice."])

            generated_choices = []
            for i, feature_name in enumerate([pipeline_result["primary_feature"], pipeline_result["secondary_feature"]]):
                bargain_options = bargain_map.get(feature_name, bargain_map["default"])
                effects_pool = bargain_options["buffs"] if is_buff else bargain_options["debuffs"]
                base_effect = random.choice(effects_pool)
                scaled_effect = base_effect.copy()
                scaled_effect["modifier"] = scale_modifier(base_effect["modifier"], pipeline_result["confidence"])
                generated_choices.append({
                    "description": descriptions[i] if i < len(descriptions) else f"Choice {i+1}",
                    "buffs": [scaled_effect] if is_buff else [],
                    "debuffs": [scaled_effect] if not is_buff else []
                })
            choices = generated_choices

        except (ValueError, json.JSONDecodeError) as e:
            # This will now catch both JSON parsing errors and the "No JSON object found" error
            logging.error(f"Gemini API call or JSON parsing failed: {e}. Using fallback dialogue/choices.")
        except Exception as e:
            # Catch any other unexpected errors
            logging.error(f"An unexpected error occurred during the LLM pipeline: {e}. Using fallback dialogue/choices.", exc_info=True)


    seer_result_payload = {
        "playerId": features.get("player_id", "unknown"),
        "encounterId": encounter_id,
        "dialogue": dialogue,
        "choices": choices
    }

    final_envelope = {
        "message_type": "seer_result_update",
        "payload": json.dumps(seer_result_payload)
    }

    if producer:
        with config_lock:
            producer_topic = APP_SETTINGS.get("kafka_topics", {}).get("producer_topic", "seer_results")
        producer.send(producer_topic, value=final_envelope)
        producer.flush()
        logging.info(f"Successfully produced Seer result envelope to Kafka for run_id: {run_id}")

def start_kafka_consumer():
    """Initializes and runs the Kafka Consumer loop in a dedicated thread."""
    logging.info("Consumer thread started. Initializing Kafka Consumer...")
    with config_lock:
        kafka_topics = APP_SETTINGS.get("kafka_topics", {})
        consumer_topic = kafka_topics.get("consumer_topic", "seer_triggers")
        group_id = kafka_topics.get("consumer_group_id", "seer-orchestrator-group")
    kafka_brokers = get_required_env_var("KAFKA_BROKERS")

    try:
        consumer = KafkaConsumer(
            consumer_topic,
            bootstrap_servers=kafka_brokers,
            group_id=group_id,
            auto_offset_reset='earliest', # Start reading from the earliest available offset
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) # Deserialize JSON messages
        )
        logging.info(f"Kafka Consumer initialized successfully on topic '{consumer_topic}'. Waiting for messages...")
        for message in consumer:
            if isinstance(message.value, dict):
                process_seer_pipeline(message.value)
    except Exception as e:
        logging.error(f"An error occurred in the Kafka consumer thread: {e}", exc_info=True)

# --- API Endpoints & Main Execution ---

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/reload-config', methods=['POST'])
def reload_config_endpoint():
    """Endpoint to trigger a configuration reload."""
    logging.info("'/reload-config' endpoint called. Attempting to reload configuration.")
    success, message = load_and_reinitialize()
    if success:
        return jsonify({"status": "success", "message": message}), 200
    else:
        return jsonify({"status": "error", "message": message}), 500

if __name__ == '__main__':
    logging.info("Flask app starting. Performing initial configuration and client initialization...")
    load_and_reinitialize(initial_load=True)

    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()

    app.run(host='0.0.0.0', port=5001)
