import os
import json
import logging
import random
import threading
import math
import yaml
from typing import Dict, Any, List

from flask import Flask, jsonify
from kafka import KafkaProducer, KafkaConsumer
import google.generativeai as genai
from google.cloud import bigquery

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
    """Executes the full BQML training and prediction pipeline.
    Returns None if the BQML client is not available or if any step fails.
    """
    # Guard Clause: Immediately return if BQML capabilities are disabled.
    if not gcp_bq_available or not bq_client:
        logging.warning("BQML pipeline is disabled. Skipping.")
        return None

    with config_lock:
        bigquery_dataset = APP_SETTINGS.get("bigquery_default_dataset", "seer_training_workspace")
    table_name = f"{boss_archetype}_training_data"
    model_name = f"{boss_archetype}_seer_model"
    model_uri = f"{bq_client.project}.{bigquery_dataset}.{model_name}"
    table_uri = f"{bq_client.project}.{bigquery_dataset}.{table_name}"

    sql_train = f"""
        CREATE OR REPLACE MODEL `{model_uri}`
        OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['outcome'], enable_global_explain=TRUE) AS
        SELECT * EXCEPT(upgrade_counts) FROM `{table_uri}` WHERE outcome IS NOT NULL;
    """
    try:
        bq_client.query(sql_train).result()
        logging.info(f"Successfully trained BQML model: {model_name}")
    except Exception as e:
        logging.error(f"BQML training failed for model {model_name}: {e}")
        return None

    feature_sql_parts = []
    for k, v in features.items():
        if k in ['run_id', 'encounterId', 'outcome', 'weight']: continue
        if k == 'upgrade_counts' and isinstance(v, dict):
            for up_k, up_v in v.items():
                feature_sql_parts.append(f"{up_v} AS upgrade_counts_{up_k}")
        else:
             feature_sql_parts.append(f"{v} AS {k}")

    if not feature_sql_parts:
        logging.error("No valid features found for prediction after filtering.")
        return None

    sql_predict = f"SELECT * FROM ML.EXPLAIN_PREDICT(MODEL `{model_uri}`, (SELECT {', '.join(feature_sql_parts)}))"
    try:
        results = list(bq_client.query(sql_predict).result())
        if not results: return None

        row = results[0]
        sorted_explanation = sorted(row.explanation, key=lambda x: abs(x.attribution), reverse=True)
        top_features = [exp.feature for exp in sorted_explanation[:2]]

        # Ensure at least two features are always present, using 'default' if not enough
        if len(top_features) < 2:
            top_features.extend(['default'] * (2 - len(top_features)))

        return {
            "prediction": "Win" if row.predicted_outcome == 1 else "Loss",
            "confidence": row.predicted_outcome_probs[0].prob,
            "primary_feature": top_features[0].replace("_", "."), # Format for LLM
            "secondary_feature": top_features[1].replace("_", "."), # Format for LLM
        }
    except Exception as e:
        logging.error(f"BQML prediction failed for model {model_name}: {e}")
        return None

# --- Core Logic ---

def process_seer_pipeline(features: dict):
    """Orchestrates the seer encounter logic from feature processing to Kafka production."""
    run_id = features.get("run_id")
    encounter_id = features.get("encounterId")
    if not run_id or not encounter_id:
        logging.error(f"Message missing run_id or encounterId. Payload: {features}")
        return

    logging.info(f"Processing pipeline for run_id: {run_id}, encounterId: {encounter_id}")

    dialogue, choices = generate_fallback_response()

    with config_lock:
        boss_archetype = APP_SETTINGS.get("default_boss_archetype", "melee")

    pipeline_result = run_bqml_pipeline(features, boss_archetype=boss_archetype)

    # Proceed with LLM only if BQML was successful AND the LLM is available.
    if pipeline_result and gcp_llm_available and llm_model:
        is_buff = pipeline_result["prediction"] == "Loss"
        prompt_type = "loss" if is_buff else "win"

        with config_lock:
            prompt_template = CONFIG['prompts'][prompt_type]
            bargain_map = CONFIG['feature_to_bargain_map']

        # Format the prompt with the top features from BQML
        prompt = prompt_template.format(
            primary_feature=pipeline_result["primary_feature"].replace("_", " "),
            secondary_feature=pipeline_result["secondary_feature"].replace("_", " ")
        )
        try:
            response_text = llm_model.generate_content(prompt).text
            llm_json = json.loads(response_text.strip("`").strip("json\n"))

            dialogue = llm_json.get("dialogue", "The vision is complete.")
            descriptions = llm_json.get("bargain_descriptions", ["A choice.", "Another choice."])

            generated_choices = []
            for i, feature_name in enumerate([pipeline_result["primary_feature"], pipeline_result["secondary_feature"]]):
                # Get bargain options from config based on feature, default if not specific
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
        except Exception as e:
            logging.error(f"Gemini API call or JSON parsing failed: {e}. Using fallback dialogue/choices.")

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
        consumer_topic = kafka_topics.get("consumer_topic", "bqml_features")
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
