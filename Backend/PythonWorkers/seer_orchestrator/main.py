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
llm_model: genai.GenerativeModel | None = None

# --- Environment Variable & Client Initialization ---

def get_env_var(key: str) -> str:
    """Gets an environment variable or exits if not found.
    This ensures critical environment variables are always present before proceeding.
    """
    value = os.environ.get(key)
    if not value:
        logging.error(f"FATAL: Environment variable '{key}' is not set.")
        exit(1) # Exit if essential environment variables are missing
    return value

# --- Flask App and Other Global Clients ---
# These clients are initialized once as they don't depend on the hot-reloadable config.
app = Flask(__name__)
gcp_project_id = get_env_var("GCP_PROJECT_ID")
bq_client = bigquery.Client(project=gcp_project_id)

# --- Configuration Loading & Client Initialization ---
def load_and_reinitialize(initial_load: bool = False) -> tuple[bool, str]:
    """Loads the YAML configuration file and re-initializes clients that depend on it.
    This function is designed to be called at startup and via the /reload-config endpoint.
    It uses a lock to ensure thread-safety during configuration updates.

    Args:
        initial_load (bool): True if this is the initial application startup load,
                             False if triggered by a reload endpoint.

    Returns:
        tuple[bool, str]: A tuple indicating success (True/False) and a descriptive message.
    """
    global CONFIG, APP_SETTINGS, producer, llm_model

    with config_lock:
        try:
            with open("config.yml", "r") as f:
                new_config = yaml.safe_load(f)
            logging.info("Configuration file 'config.yml' loaded successfully.")
        except FileNotFoundError:
            error_msg = "FATAL: 'config.yml' not found. Please ensure it is mounted correctly."
            logging.error(error_msg)
            if initial_load:
                exit(1) # Exit only on the first load attempt if config is missing
            return False, error_msg
        except yaml.YAMLError as e:
            error_msg = f"FATAL: Error parsing 'config.yml': {e}"
            logging.error(error_msg)
            if initial_load:
                exit(1) # Exit only on the first load attempt if config is malformed
            return False, error_msg

        # Update global configuration
        CONFIG = new_config
        APP_SETTINGS = CONFIG.get('app_settings', {})

        # Re-initialize clients that depend on the config
        try:
            # Re-init Kafka Producer (its topic or retry settings might have changed)
            kafka_brokers = get_env_var("KAFKA_BROKERS")
            # Close existing producer if it exists to release resources
            if producer:
                producer.close()
                logging.info("Existing Kafka Producer closed.")

            producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                bootstrap_servers=kafka_brokers,
                retries=APP_SETTINGS.get("kafka_producer_retries", 5) # Use config for retries
            )
            logging.info("Kafka Producer re-initialized successfully.")

            # Re-init Generative Model (model name or API key might have changed)
            gemini_api_key = get_env_var("GEMINI_API_KEY")
            genai.configure(api_key=gemini_api_key)
            llm_model = genai.GenerativeModel(APP_SETTINGS.get("gemini_model", "gemini-1.5-flash"))
            logging.info("Gemini Model re-initialized successfully.")

        except Exception as e:
            error_msg = f"Failed to re-initialize clients: {e}"
            logging.error(error_msg)
            if initial_load:
                exit(1) # Critical failure on initial load
            return False, error_msg

    return True, "Configuration reloaded successfully"

# --- Helper Functions ---

def scale_modifier(base_modifier: float, confidence: float) -> float:
    """Scales a bargain's power based on the prediction confidence.
    Higher confidence in a prediction leads to a more impactful bargain.
    Compares to a base multiplier to adjust the effect magnitude.
    """
    with config_lock: # Access CONFIG safely
        scaling_factor = CONFIG.get('confidence_scaling_factor', 1.5)

    # Ensure confidence is at least 0.5 to prevent negative or zero scaling
    if confidence < 0.5:
        confidence = 0.5

    # Normalize confidence from [0.5, 1.0] to [0, 1] for scaling calculation
    normalized_confidence = (confidence - 0.5) * 2

    # Apply a squared scaling factor for a non-linear increase in effect
    scaled_effect = base_modifier * (1 + (scaling_factor * math.pow(normalized_confidence, 2)))
    return round(scaled_effect, 3)

def generate_fallback_response() -> tuple[str, list]:
    """Creates a generic fallback response when the main pipeline (BQML/LLM) fails.
    This ensures a graceful degradation of service and provides a default user experience.
    """
    logging.warning("Generating a fallback seer response.")
    dialogue = "The visions are... unclear. The threads of fate refuse to be read. I can offer you a choice, but its nature is as murky as the future itself."
    with config_lock: # Access CONFIG safely
        fallback_options = CONFIG.get('fallback_choices', {})

    # Randomly select a pair of buffs or debuffs as fallback choices
    choices = random.sample(fallback_options.get('buffs', []), 2) if random.choice([True, False]) else \
              random.sample(fallback_options.get('debuffs', []), 2)
    return dialogue, choices

def run_bqml_pipeline(features: dict, boss_archetype: str) -> Dict[str, Any] | None:
    """Executes the full BQML training and prediction pipeline.
    This involves training a logistic regression model on historical data and
    then using it to predict the outcome and provide explanations for new features.

    Args:
        features (dict): A dictionary of features for the prediction.
        boss_archetype (str): The archetype of the boss, used to determine
                              the specific BigQuery table and model names.

    Returns:
        Dict[str, Any] | None: A dictionary containing prediction details (win/loss,
                               confidence, primary/secondary features) or None if an error occurs.
    """
    # Dynamically get BigQuery dataset from env or config
    bigquery_dataset = os.environ.get("BIGQUERY_DATASET", APP_SETTINGS.get("bigquery_default_dataset", "seer_training_workspace"))
    table_name = f"{boss_archetype}_training_data"
    model_name = f"{boss_archetype}_seer_model"
    model_uri = f"{gcp_project_id}.{bigquery_dataset}.{model_name}"
    table_uri = f"{gcp_project_id}.{bigquery_dataset}.{table_name}"

    # 1. Train model (or re-train if it exists)
    # Uses Logistic Regression model suitable for binary classification (win/loss)
    sql_train = f"""
        CREATE OR REPLACE MODEL `{model_uri}`
        OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['outcome'], enable_global_explain=TRUE) AS
        SELECT * EXCEPT(upgrade_counts)
        FROM `{table_uri}`
        WHERE outcome IS NOT NULL;
    """
    try:
        bq_client.query(sql_train).result()
        logging.info(f"Successfully trained BQML model: {model_name}")
    except Exception as e:
        logging.error(f"BQML training failed for model {model_name}: {e}")
        return None

    # 2. Prepare features for prediction and get explanation
    feature_sql_parts = []
    for k, v in features.items():
        # Skip internal or non-feature keys
        if k in ['run_id', 'encounterId', 'outcome', 'weight']:
            continue
        # Handle nested upgrade counts specifically
        if k == 'upgrade_counts' and isinstance(v, dict):
            for up_k, up_v in v.items():
                feature_sql_parts.append(f"{up_v} AS upgrade_counts_{up_k}")
        else:
             feature_sql_parts.append(f"{v} AS {k}")

    if not feature_sql_parts:
        logging.error("No valid features found for prediction after filtering.")
        return None

    # Use ML.EXPLAIN_PREDICT to get prediction and feature attributions
    sql_predict = f"SELECT * FROM ML.EXPLAIN_PREDICT(MODEL `{model_uri}`, (SELECT {', '.join(feature_sql_parts)}))"
    try:
        results = list(bq_client.query(sql_predict).result())
        if not results:
            logging.warning(f"BQML prediction returned no results for model {model_name}.")
            return None

        row = results[0]
        # Sort explanation by absolute attribution to find most impactful features
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
    """Orchestrates the seer encounter logic from feature processing to Kafka production.
    This is the main business logic flow, handling prediction, LLM interaction,
    bargain generation, and producing the final result to Kafka.
    """
    run_id = features.get("run_id")
    encounter_id = features.get("encounterId")
    if not run_id or not encounter_id:
        logging.error(f"Message missing run_id or encounterId. Payload: {features}")
        return

    logging.info(f"Processing pipeline for run_id: {run_id}, encounterId: {encounter_id}")

    # Default to fallback response, override on successful pipeline execution
    dialogue, choices = generate_fallback_response()

    with config_lock: # Access APP_SETTINGS safely
        boss_archetype = APP_SETTINGS.get("default_boss_archetype", "melee")

    pipeline_result = run_bqml_pipeline(features, boss_archetype=boss_archetype)

    # Proceed with LLM and choice generation only if BQML pipeline was successful and LLM is initialized
    if pipeline_result and llm_model:
        is_buff = pipeline_result["prediction"] == "Loss" # If predicted loss, offer buff
        prompt_type = "loss" if is_buff else "win"

        with config_lock: # Access CONFIG for prompt templates and bargain map safely
            prompt_template = CONFIG['prompts'][prompt_type]
            bargain_map = CONFIG['feature_to_bargain_map']

        # Format the prompt with the top features from BQML
        prompt = prompt_template.format(
            primary_feature=pipeline_result["primary_feature"].replace("_", " "),
            secondary_feature=pipeline_result["secondary_feature"].replace("_", " ")
        )
        try:
            # Call Gemini API to generate dialogue and bargain descriptions
            response_text = llm_model.generate_content(prompt).text
            # Clean and parse the JSON response from the LLM
            llm_json = json.loads(response_text.strip("`").strip("json\n"))

            dialogue = llm_json.get("dialogue", "The vision is complete.")
            descriptions = llm_json.get("bargain_descriptions", ["A choice.", "Another choice."])

            # Generate actual choices based on LLM output and scaled effects
            generated_choices = []
            for i, feature_name in enumerate([pipeline_result["primary_feature"], pipeline_result["secondary_feature"]]):
                # Get bargain options from config based on feature, default if not specific
                bargain_options = bargain_map.get(feature_name, bargain_map["default"])

                # Select from buffs or debuffs pool based on prediction
                effects_pool = bargain_options["buffs"] if is_buff else bargain_options["debuffs"]
                base_effect = random.choice(effects_pool)

                # Scale the effect modifier based on prediction confidence
                scaled_effect = base_effect.copy()
                scaled_effect["modifier"] = scale_modifier(base_effect["modifier"], pipeline_result["confidence"])

                generated_choices.append({
                    "description": descriptions[i] if i < len(descriptions) else f"Choice {i+1}", # Use LLM desc, fallback if needed
                    "buffs": [scaled_effect] if is_buff else [],
                    "debuffs": [scaled_effect] if not is_buff else []
                })
            choices = generated_choices # Overwrite fallback choices with generated ones

        except Exception as e:
            logging.error(f"Gemini API call or JSON parsing failed: {e}. Using fallback response.")
            # If LLM fails, the initially generated fallback response will be used.

    # Assemble and produce final result payload to Kafka
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

    # Ensure producer is initialized before sending messages
    if producer:
        with config_lock: # Access APP_SETTINGS safely for topic name
            producer_topic = APP_SETTINGS.get("kafka_topics", {}).get("producer_topic", "seer_results")

        producer.send(producer_topic, value=final_envelope)
        producer.flush() # Ensure message is sent immediately
        logging.info(f"Successfully produced Seer result envelope to Kafka for run_id: {run_id}")
    else:
        logging.error(f"Kafka Producer not initialized. Failed to send message for run_id: {run_id}")

def start_kafka_consumer():
    """Initializes and runs the Kafka Consumer loop in a dedicated thread.
    This consumer listens for incoming feature messages and processes them
    through the seer pipeline.
    """
    logging.info("Consumer thread started. Initializing Kafka Consumer...")

    with config_lock: # Access APP_SETTINGS safely for Kafka topic and group ID
        kafka_topics = APP_SETTINGS.get("kafka_topics", {})
        consumer_topic = kafka_topics.get("consumer_topic", "bqml_features")
        group_id = kafka_topics.get("consumer_group_id", "seer-orchestrator-group")

    kafka_brokers = get_env_var("KAFKA_BROKERS") # Kafka brokers are from environment variables

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
            else:
                logging.warning(f"Received message with unexpected format: {message.value}")
    except Exception as e:
        logging.error(f"An error occurred in the Kafka consumer thread: {e}", exc_info=True)

# --- API Endpoints & Main Execution ---

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/reload-config', methods=['POST'])
def reload_config_endpoint():
    """Endpoint to trigger a configuration reload.
    Allows for dynamic updates to configuration without restarting the application,
    facilitating maintenance and updates.
    """
    logging.info("'/reload-config' endpoint called. Attempting to reload configuration.")
    success, message = load_and_reinitialize()
    if success:
        return jsonify({"status": "success", "message": message}), 200
    else:
        return jsonify({"status": "error", "message": message}), 500

if __name__ == '__main__':
    # Initial load of configuration and client initialization at startup
    logging.info("Flask app starting. Performing initial configuration load...")
    load_and_reinitialize(initial_load=True)

    # Start the background thread for the Kafka consumer
    # The consumer runs in a daemon thread, so it will exit when the main program exits.
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()
    logging.info("Kafka consumer thread started.")

    # Start the Flask web server
    # This will block the main thread and serve API requests.
    app.run(host='0.0.0.0', port=5001)
    logging.info("Flask app started and listening on 0.0.0.0:5001.")