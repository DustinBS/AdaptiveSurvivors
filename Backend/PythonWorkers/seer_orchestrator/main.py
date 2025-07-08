# /Backend/PythonWorkers/seer_orchestrator/main.py

import os
import json
import logging
import random
import threading
import time
import math
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
import google.generativeai as genai
from google.cloud import bigquery

# --- Configuration & Initialization ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "seer_training_workspace")
CONFIDENCE_SCALING_FACTOR = 1.5 # Controls how dramatically bargain strength increases with confidence

app = Flask(__name__)
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), bootstrap_servers=KAFKA_BROKERS, retries=5)
genai.configure(api_key=GEMINI_API_KEY)
llm_model = genai.GenerativeModel('gemini-1.5-flash')
bq_client = bigquery.Client(project=GCP_PROJECT_ID)
CACHE_ITEM_TTL_SECONDS = 3600 # Clear feature vectors after 1 hour

# --- Thread-Safe Cache ---
feature_cache = {}
cache_lock = threading.Lock()

# Each feature maps to a LIST of possible effects for variety.
FEATURE_TO_BARGAIN_MAP = {
    "total_dashes": {
        "buffs": [
            {"targetStat": "DashCooldown", "modifier": -0.15, "isPercentage": True, "duration": 180},
            {"targetStat": "MoveSpeed", "modifier": 0.10, "isPercentage": True, "duration": 180}
        ],
        "debuffs": [
            {"targetStat": "DashCooldown", "modifier": 0.20, "isPercentage": True, "duration": 180},
            {"targetStat": "MoveSpeed", "modifier": -0.10, "isPercentage": True, "duration": 180}
        ]
    },
    "total_damage_dealt": {
        "buffs": [{"targetStat": "AttackDamage", "modifier": 0.15, "isPercentage": True, "duration": 180}],
        "debuffs": [{"targetStat": "AttackDamage", "modifier": -0.20, "isPercentage": True, "duration": 180}]
    },
    "total_damage_taken": {
        "buffs": [
            {"targetStat": "MaxHealth", "modifier": 0.25, "isPercentage": True, "duration": 180},
            {"targetStat": "Armor", "modifier": 20, "isPercentage": False, "duration": 180}
        ],
        "debuffs": [
            {"targetStat": "MaxHealth", "modifier": -0.20, "isPercentage": True, "duration": 180},
            {"targetStat": "Armor", "modifier": -15, "isPercentage": False, "duration": 180}
        ]
    },
    "damage_taken_from_elites": {
        "buffs": [{"targetStat": "Armor", "modifier": 30, "isPercentage": False, "duration": 180}],
        "debuffs": [{"targetStat": "AttackSpeed", "modifier": -0.15, "isPercentage": True, "duration": 180}]
    },
    "avg_hp_percent": {
        "buffs": [{"targetStat": "MaxHealth", "modifier": 0.20, "isPercentage": True, "duration": 180}],
        "debuffs": [{"targetStat": "MoveSpeed", "modifier": -0.15, "isPercentage": True, "duration": 180}]
    },
    # Example for handling specific upgrades from the 'upgrade_counts' feature map
    "upgrade_counts.sword": {
        "buffs": [{"targetStat": "AttackDamage", "modifier": 0.10, "isPercentage": True, "duration": 180}],
        "debuffs": [{"targetStat": "AttackSpeed", "modifier": -0.10, "isPercentage": True, "duration": 180}]
    },
    "upgrade_counts.boots": {
        "buffs": [{"targetStat": "MoveSpeed", "modifier": 0.10, "isPercentage": True, "duration": 180}],
        "debuffs": [{"targetStat": "DashCooldown", "modifier": 0.10, "isPercentage": True, "duration": 180}]
    },
    "default": { # A fallback for any unmapped feature
        "buffs": [{"targetStat": "AttackSpeed", "modifier": 0.10, "isPercentage": True, "duration": 180}],
        "debuffs": [{"targetStat": "Armor", "modifier": -10, "isPercentage": False, "duration": 180}]
    }
}
WIN_PROMPT_TEMPLATE = """
You are a cryptic, all-knowing Seer. A player is about to face a boss. An oracle has predicted they will WIN.
The primary reason for this prediction is their performance related to: '{primary_feature}'.
The secondary reason is: '{secondary_feature}'.

Generate a JSON object with two keys: "dialogue" and "bargain_descriptions".
- "dialogue": A short, 2-sentence prophecy that sounds confident and hints at why they will win, based on the primary feature. It must lead into offering a challenge.
- "bargain_descriptions": A list of two strings. The first string should describe a challenge (a debuff) related to the primary feature. The second string should describe a challenge related to the secondary feature.
"""

LOSS_PROMPT_TEMPLATE = """
You are a cryptic, all-knowing Seer. A player is about to face a boss. An oracle has predicted they will LOSE.
The primary reason for this prediction is their performance related to: '{primary_feature}'.
The secondary reason is: '{secondary_feature}'.

Generate a JSON object with two keys: "dialogue" and "bargain_descriptions".
- "dialogue": A short, 2-sentence prophecy that sounds ominous and hints at why they will lose, based on the primary feature. It must lead into offering a boon.
- "bargain_descriptions": A list of two strings. The first string should describe a helpful boon (a buff) related to the primary feature. The second string should describe a boon related to the secondary feature.
"""
FALLBACK_BUFFS = [
    {"description": "A sliver of fortitude. (+20% Max HP)", "buffs": [{"targetStat": "MaxHealth", "modifier": 0.20, "isPercentage": True, "duration": 180}], "debuffs": []},
    {"description": "A fleeting burst of speed. (+10% Move Speed)", "buffs": [{"targetStat": "MoveSpeed", "modifier": 0.10, "isPercentage": True, "duration": 180}], "debuffs": []},
    {"description": "A moment of clarity. (-15% Dash Cooldown)", "buffs": [{"targetStat": "DashCooldown", "modifier": -0.15, "isPercentage": True, "duration": 180}], "debuffs": []},
    {"description": "A surge of inner power. (+10% Attack Damage)", "buffs": [{"targetStat": "AttackDamage", "modifier": 0.10, "isPercentage": True, "duration": 180}], "debuffs": []}
]
FALLBACK_DEBUFFS = [
    {"description": "A true test of skill. (-15% Attack Damage)", "buffs": [], "debuffs": [{"targetStat": "AttackDamage", "modifier": -0.15, "isPercentage": True, "duration": 180}]},
    {"description": "An unexpected burden. (+20% Dash Cooldown)", "buffs": [], "debuffs": [{"targetStat": "DashCooldown", "modifier": 0.20, "isPercentage": True, "duration": 180}]},
    {"description": "A heavy heart. (-15% Move Speed)", "buffs": [], "debuffs": [{"targetStat": "MoveSpeed", "modifier": -0.15, "isPercentage": True, "duration": 180}]},
    {"description": "A crack in your defenses. (-15 Armor)", "buffs": [], "debuffs": [{"targetStat": "Armor", "modifier": -15, "isPercentage": False, "duration": 180}]}
]

# --- Background Kafka Consumer (Unchanged) ---
def consume_features():
    consumer = KafkaConsumer('bqml_features',bootstrap_servers=KAFKA_BROKERS,auto_offset_reset='latest',value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    logging.info("Kafka consumer thread started...")

    last_cleanup_time = time.time()

    for message in consumer:
        # --- Add items to cache ---
        try:
            run_id = message.value.get("run_id")
            if run_id:
                with cache_lock:
                    feature_cache[run_id] = (message.value, time.time()) # Store with a timestamp
                logging.info(f"Cached features for run_id: {run_id}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")

        # --- Periodically clean old items from cache ---
        if time.time() - last_cleanup_time > 600: # Clean every 10 minutes
            with cache_lock:
                now = time.time()
                keys_to_delete = [
                    run_id for run_id, (_, ts) in feature_cache.items()
                    if now - ts > CACHE_ITEM_TTL_SECONDS
                ]
                for run_id in keys_to_delete:
                    del feature_cache[run_id]
                if keys_to_delete:
                    logging.info(f"Cache cleanup: removed {len(keys_to_delete)} old entries.")
            last_cleanup_time = time.time()

# --- Helper Functions ---

def scale_modifier(base_modifier: float, confidence: float) -> float:
    """Scales a bargain's power based on the prediction confidence."""
    # Normalize confidence from [0.5, 1.0] to [0.0, 1.0]
    if confidence < 0.5: confidence = 0.5
    normalized_confidence = (confidence - 0.5) * 2

    # Apply a non-linear scaling for a more dramatic effect
    scaled_effect = base_modifier * (1 + (CONFIDENCE_SCALING_FACTOR * math.pow(normalized_confidence, 2)))
    # Round to 3 decimal places for cleanliness
    return round(scaled_effect, 3)

def generate_fallback_response():
    # This function is unchanged
    logging.warning("Generating a fallback seer response.")
    dialogue = "The visions are... unclear. The threads of fate refuse to be read. I can offer you a choice, but its nature is as murky as the future itself."
    choices = random.sample(FALLBACK_BUFFS, 2) if random.choice([True, False]) else random.sample(FALLBACK_DEBUFFS, 2)
    return dialogue, choices

def run_bqml_pipeline(features: dict, boss_archetype: str):
    """Executes the full BQML training and prediction pipeline."""
    table_name = f"{boss_archetype}_training_data"
    model_name = f"{boss_archetype}_seer_model"

    # 1. Train model
    sql_train = f"CREATE OR REPLACE MODEL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{model_name}` OPTIONS(model_type='LOGISTIC_REG', input_label_cols=['outcome'], enable_global_explain=TRUE) AS SELECT * FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}` WHERE outcome IS NOT NULL;"
    try:
        bq_client.query(sql_train).result()
        logging.info(f"Successfully trained BQML model: {model_name}")
    except Exception as e:
        logging.error(f"BQML training failed for model {model_name}: {e}")
        return None

    # 2. Get prediction and explanation
    feature_sql_parts = []
    # Handle the nested upgrade_counts map
    for k, v in features.items():
        if k == 'upgrade_counts':
            if isinstance(v, dict):
                for up_k, up_v in v.items():
                    feature_sql_parts.append(f"{up_v} AS upgrade_counts.{up_k}")
        elif k != 'run_id':
             feature_sql_parts.append(f"{v} AS {k}")

    sql_predict = f"SELECT * FROM ML.EXPLAIN_PREDICT(MODEL `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{model_name}`, (SELECT {', '.join(feature_sql_parts)}))"
    try:
        results = bq_client.query(sql_predict).result()
        for row in results: # Should only be one row
            sorted_explanation = sorted(row.explanation, key=lambda x: abs(x.attribution), reverse=True)
            top_features = [exp.feature for exp in sorted_explanation[:2]]

            if len(top_features) < 2:
                top_features.extend(['default'] * (2 - len(top_features)))

            return {
                "prediction": "Win" if row.predicted_outcome == 1 else "Loss",
                "confidence": row.predicted_outcome_probs[0].prob,
                "primary_feature": top_features[0],
                "secondary_feature": top_features[1]
            }
    except Exception as e:
        logging.error(f"BQML prediction failed for model {model_name}: {e}")
        return None

# --- API Endpoint ---
@app.route('/trigger', methods=['POST'])
def trigger_seer_pipeline():
    data = request.get_json()
    run_id = data.get("run_id")
    boss_archetype = "melee" # Default, should be passed from Flink
    if not run_id:
        return jsonify({"status": "error", "message": "Missing 'run_id'"}), 400

    logging.info(f"Orchestrator triggered for run_id: {run_id}")

    # Poll the cache for the feature vector
    features = None
    for _ in range(10):
        with cache_lock:
            cached_item = feature_cache.get(run_id)
            if cached_item:
                features = cached_item[0] # Get the feature data, ignore the timestamp
                break
        time.sleep(0.2)

    if not features:
        dialogue, choices = generate_fallback_response()
    else:
        pipeline_result = run_bqml_pipeline(features, boss_archetype)
        if not pipeline_result:
            dialogue, choices = generate_fallback_response()
        else:
            prompt_template = WIN_PROMPT_TEMPLATE if pipeline_result["prediction"] == "Win" else LOSS_PROMPT_TEMPLATE
            prompt = prompt_template.format(primary_feature=pipeline_result["primary_feature"].replace("_", " "), secondary_feature=pipeline_result["secondary_feature"].replace("_", " "))

            try:
                response_text = llm_model.generate_content(prompt).text
                llm_json = json.loads(response_text.strip("`").strip("json\n"))
                dialogue = llm_json.get("dialogue", "The vision is complete.")
                descriptions = llm_json.get("bargain_descriptions", ["A choice.", "Another choice."])
            except Exception as e:
                logging.error(f"Gemini API call or JSON parsing failed: {e}")
                dialogue, choices = generate_fallback_response()
            else:
                choices = []
                is_buff = pipeline_result["prediction"] == "Loss"
                for i, feature_name in enumerate([pipeline_result["primary_feature"], pipeline_result["secondary_feature"]]):
                    bargain_options = FEATURE_TO_BARGAIN_MAP.get(feature_name, FEATURE_TO_BARGAIN_MAP["default"])

                    # Choose a random effect from the list of possibilities for that feature
                    base_effect = random.choice(bargain_options["buffs"] if is_buff else bargain_options["debuffs"])

                    # Create a copy to modify
                    scaled_effect = base_effect.copy()

                    # Scale the modifier based on confidence
                    scaled_effect["modifier"] = scale_modifier(base_effect["modifier"], pipeline_result["confidence"])

                    choices.append({
                        "description": descriptions[i],
                        "buffs": [scaled_effect] if is_buff else [],
                        "debuffs": [scaled_effect] if not is_buff else []
                    })

    # Produce final result to Kafka
    # Note: `encounterId` should ideally be passed from Flink in the trigger request body.
    final_payload = { "playerId": features.get("player_id", "unknown") if features else "unknown", "encounterId": "0", "dialogue": dialogue, "choices": choices }
    producer.send("seer_results", value=final_payload)
    producer.flush()
    logging.info(f"Successfully produced Seer result to Kafka for run_id: {run_id}")
    return jsonify({"status": "success", "message": "Seer result produced"}), 200

# --- Main Execution ---
if __name__ == '__main__':
    consumer_thread = threading.Thread(target=consume_features, daemon=True)
    consumer_thread.start()
    app.run(host='0.0.0.0', port=5001)