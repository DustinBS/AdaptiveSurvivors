# CloudFunctions/llm_npc_commentary/main.py

# This file implements the Google Cloud Function for generating LLM-based NPC commentary.
# It is triggered by HTTP requests, fetches player run summaries from BigQuery,
# constructs a prompt for the Gemini API, and returns the generated dialogue.

import os
import functions_framework
from flask import jsonify
from google.cloud import bigquery
from vertexai.generative_models import GenerativeModel, Part, HarmCategory, HarmBlockThreshold

# Initialize BigQuery client
bq_client = bigquery.Client()

# Get the Gemini model name from environment variables
# Defaults to 'gemini-1.5-flash' if not set
GEMINI_MODEL_NAME = os.environ.get("GEMINI_MODEL_NAME", "gemini-1.5-flash")

@functions_framework.http
def generate_npc_commentary(request):
    """
    HTTP Cloud Function that generates NPC commentary using the Gemini API.

    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/2.3.x/genindex/#flask.Request>
        The request body should be JSON and contain:
        - player_id: The ID of the player whose run summary is needed.
        - npc_personality: A string describing the NPC's personality (e.g., "wise old sage", "sarcastic merchant").

    Returns:
        flask.Response: The API response, which contains the generated commentary.
    """
    request_json = request.get_json(silent=True)

    if not request_json:
        return jsonify({"error": "No JSON data provided in the request body."}), 400

    player_id = request_json.get("player_id")
    npc_personality = request_json.get("npc_personality")

    if not player_id or not npc_personality:
        return jsonify({"error": "Missing 'player_id' or 'npc_personality' in request body."}), 400

    # For demonstration, we'll use a placeholder for run summary.
    # In a real scenario, you'd fetch this from BigQuery's `llm_npc_prompts` table.
    # The GDD mentions: "Function reads run summary from llm_npc_prompts BigQuery table"

    # Example BigQuery query (adjust based on your actual BigQuery schema)
    # This query assumes `llm_npc_prompts` table contains a column named `player_id`
    # and another column named `run_summary_json` which holds the summarized data
    # as a JSON string.
    query = f"""
        SELECT
            run_summary_json
        FROM
            `{bq_client.project}.game_data.llm_npc_prompts`
        WHERE
            player_id = @player_id
        ORDER BY
            timestamp DESC
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("player_id", "STRING", player_id),
        ]
    )

    player_run_summary = ""
    try:
        query_job = bq_client.query(query, job_config=job_config)
        results = query_job.result() # Waits for job to complete.
        
        for row in results:
            player_run_summary = row["run_summary_json"]
            break # Get only the first (latest) summary
        
        if not player_run_summary:
            print(f"No run summary found for player_id: {player_id}")
            player_run_summary = "The player had a normal, unremarkable run." # Default if no summary found

    except Exception as e:
        print(f"Error fetching run summary from BigQuery: {e}")
        # Fallback to a generic summary or handle as an error
        player_run_summary = "The player's recent actions are mysterious, but they surely survived something extraordinary."


    # Construct the prompt for the LLM
    # The GDD states: "prompt tailored to the NPC personality + player run summary"
    prompt_text = f"""
    You are an NPC with the following personality: "{npc_personality}".
    The player has just completed a run. Here is a summary of their performance and actions:
    {player_run_summary}

    Based on your personality and the player's run summary, provide a short (2-3 sentences), personalized commentary.
    Make your commentary engaging and reflective of the player's experience.
    """

    try:
        # Initialize the Generative Model
        model = GenerativeModel(GEMINI_MODEL_NAME)

        # Generate content with safety settings (example settings)
        # Adjust safety settings based on your application's requirements
        response = model.generate_content(
            [Part.from_text(prompt_text)],
            safety_settings={
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
            },
            # Optional: configure generation parameters if needed
            # generation_config={"temperature": 0.7, "max_output_tokens": 150}
        )

        commentary = response.text.strip()
        return jsonify({"commentary": commentary}), 200

    except Exception as e:
        print(f"Error generating commentary with Gemini API: {e}")
        return jsonify({"error": f"Failed to generate commentary: {e}"}), 500

