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

# --- Initial Setup & Configuration Loading ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config() -> Dict[str, Any]:
    """Loads the YAML configuration file."""
    try:
        with open("config.yml", "r") as f:
            config = yaml.safe_load(f)
            logging.info("Configuration file 'config.yml' loaded successfully.")
            return config
    except FileNotFoundError:
        logging.error("FATAL: 'config.yml' not found. Please ensure it is mounted correctly.")
        exit(1) # Exit if the config is missing
    except yaml.YAMLError as e:
        logging.error(f"FATAL: Error parsing 'config.yml': {e}")
        exit(1)

CONFIG = load_config()

# --- Environment Variable & Client Initialization ---

def get_env_var(key: str) -> str:
    """Gets an environment variable or exits if not found."""
    value = os.environ.get(key)
    if not value:
        logging.error(f"FATAL: Environment variable '{key}' is not set.")
        exit(1)
    return value

KAFKA_BROKERS = get_env_var("KAFKA_BROKERS")
GEMINI_API_KEY = get_env_var("GEMINI_API_KEY")
GCP_PROJECT_ID = get_env_var("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "seer_training_workspace")

# Initialize clients
app = Flask(__name__)
producer = KafkaProducer(
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    bootstrap_servers=KAFKA_BROKERS,
    retries=5
)
genai.configure(api_key=GEMINI_API_KEY)
llm_model = genai.GenerativeModel('gemini-1.5-flash')
bq_client = bigquery.Client(project=GCP_PROJECT_ID)

# --- Helper Functions ---

def scale_modifier(base_modifier: float, confidence: float) -> float:
    """Scales a bargain's power based on the prediction confidence."""
    if confidence < 0.5: confidence = 0.5
    normalized_confidence = (confidence - 0.5) * 2
    scaling_factor = CONFIG.get('confidence_scaling_factor', 1.5)
    scaled_effect = base_modifier * (1 + (scaling_factor * math.pow(normalized_confidence, 2)))
    return round(scaled_effect, 3)

def generate_fallback_response() -> tuple[str, list]:
    """Creates a generic fallback response when the main pipeline fails."""
    logging.warning("Generating a fallback seer response.")
    dialogue = "The visions are... unclear. The threads of fate refuse to be read. I can offer you a choice, but its nature is as murky as the future itself."
    fallback_options = CONFIG.get('fallback_choices', {})
    choices = random.sample(fallback_options.get('buffs', []), 2) if random.choice([True, False]) else random.sample(fallback_options.get('debuffs', []), 2)
    return dialogue, choices

def run_bqml_pipeline(features: dict, boss_archetype: str) -> Dict[str, Any] | None:
    """Executes the full BQML training and prediction pipeline."""
    table_name = f"{boss_archetype}_training_data"
    model_name = f"{boss_archetype}_seer_model"
    model_uri = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{model_name}"
    table_uri = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

    # 1. Train model
    sql_train = f"CREATE OR REPLACE MODEL `{model_uri}` OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['outcome'], enable_global_explain=TRUE) AS SELECT * FROM `{table_uri}` WHERE outcome IS NOT NULL;"
    try:
        bq_client.query(sql_train).result()
        logging.info(f"Successfully trained BQML model: {model_name}")
    except Exception as e:
        logging.error(f"BQML training failed for model {model_name}: {e}")
        return None

    # 2. Get prediction and explanation
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
        if len(top_features) < 2: top_features.extend(['default'] * (2 - len(top_features)))

        return {
            "prediction": "Win" if row.predicted_outcome == 1 else "Loss",
            "confidence": row.predicted_outcome_probs[0].prob,
            "primary_feature": top_features[0].replace("_", "."),
            "secondary_feature": top_features[1].replace("_", "."),
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

    # Default to fallback, override on success
    dialogue, choices = generate_fallback_response()

    pipeline_result = run_bqml_pipeline(features, boss_archetype="melee")
    if pipeline_result:
        is_buff = pipeline_result["prediction"] == "Loss"
        prompt_type = "loss" if is_buff else "win"
        prompt_template = CONFIG['prompts'][prompt_type]
        prompt = prompt_template.format(
            primary_feature=pipeline_result["primary_feature"].replace("_", " "),
            secondary_feature=pipeline_result["secondary_feature"].replace("_", " ")
        )
        try:
            response_text = llm_model.generate_content(prompt).text
            llm_json = json.loads(response_text.strip("`").strip("json\n"))

            dialogue = llm_json.get("dialogue", "The vision is complete.")
            descriptions = llm_json.get("bargain_descriptions", ["A choice.", "Another choice."])

            # If LLM succeeds, generate real choices
            generated_choices = []
            bargain_map = CONFIG['feature_to_bargain_map']
            for i, feature_name in enumerate([pipeline_result["primary_feature"], pipeline_result["secondary_feature"]]):
                bargain_options = bargain_map.get(feature_name, bargain_map["default"])

                effects_pool = bargain_options["buffs"] if is_buff else bargain_options["debuffs"]
                base_effect = random.choice(effects_pool)

                scaled_effect = base_effect.copy()
                scaled_effect["modifier"] = scale_modifier(base_effect["modifier"], pipeline_result["confidence"])

                generated_choices.append({
                    "description": descriptions[i],
                    "buffs": [scaled_effect] if is_buff else [],
                    "debuffs": [scaled_effect] if not is_buff else []
                })
            choices = generated_choices # Overwrite fallback choices

        except Exception as e:
            logging.error(f"Gemini API call or JSON parsing failed: {e}. Using fallback response.")
            # Let the already-generated fallback response be used

    # Assemble and produce final result to Kafka
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

    producer.send("seer_results", value=final_envelope)
    producer.flush()
    logging.info(f"Successfully produced Seer result envelope to Kafka for run_id: {run_id}")

def start_kafka_consumer():
    """Initializes and runs the Kafka Consumer loop."""
    logging.info("Consumer thread started. Initializing Kafka Consumer...")
    try:
        consumer = KafkaConsumer(
            'bqml_features',
            bootstrap_servers=KAFKA_BROKERS,
            group_id='seer-orchestrator-group',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logging.info("Kafka Consumer initialized successfully. Waiting for messages...")
        for message in consumer:
            if isinstance(message.value, dict):
                process_seer_pipeline(message.value)
            else:
                logging.warning(f"Received message with unexpected format: {message.value}")
    except Exception as e:
        logging.error(f"An error occurred in the Kafka consumer thread: {e}", exc_info=True)

# --- API Endpoint & Main Execution ---

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    logging.info("Flask app starting. Creating and starting consumer thread...")
    consumer_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    consumer_thread.start()
    app.run(host='0.0.0.0', port=5001)