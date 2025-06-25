# CloudFunctions/post_run_commentary_function/main.py

import functions_framework
import google.generativeai as genai
import os
import json
from flask import make_response

# It's best practice to load the API key from an environment variable
# for security and portability.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable not set.")

genai.configure(api_key=GEMINI_API_KEY)

# Configuration for the generative model
# Using flash for speed and cost-effectiveness, as specified in the GDD. 
generation_config = {
    "temperature": 0.8,
    "top_p": 1.0,
    "top_k": 32,
    "max_output_tokens": 150,
}
model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    generation_config=generation_config
)

@functions_framework.http
def post_run_commentary_function(request):
    """
    HTTP Cloud Function to generate NPC commentary based on player run data.
    Triggered by a POST request from the Unity client. 
    """
    # Set CORS headers for the preflight request and the main request
    # This is crucial for allowing requests from your Unity game client.
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    headers = {'Access-Control-Allow-Origin': '*'}

    # Ensure the request is a POST request with JSON data
    if request.method != 'POST':
        return (f"Method {request.method} not allowed. Use POST.", 405, headers)

    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return ("Invalid JSON in request body.", 400, headers)

        # Extract data from the new payload structure
        personality = request_json.get("npc_personality", "a generic video game character")
        stat_data = request_json.get("statistic_to_comment_on")
        
        if not stat_data or "Key" not in stat_data or "Value" not in stat_data:
             return ("'statistic_to_comment_on' field is missing or malformed.", 400, headers)
        
        stat_key = stat_data["Key"]
        stat_value = stat_data["Value"]

    except Exception as e:
        return (f"Error parsing request JSON: {e}", 400, headers)

    # Construct a more focused prompt for the Gemini API based on the single statistic 
    prompt_parts = [
        f"You are a video game NPC with this personality: '{personality}'.",
        "Based on the following single fact about a player, provide a short, flavorful, in-character comment (2-3 sentences max).",
        "Feel free to state the fact literally or allude to it naturally in your comment.",
        "For example, if the fact is 'enemies_killed_total: 50', you might say 'You fought with the fury of a wild lion! So much carnage in such a short time.' to naturally allude to it",
        f"Fact to comment on: '{stat_key}: {stat_value}'"
    ]
    prompt = "\n".join(prompt_parts)

    try:
        # Generate content using the Gemini API
        response = model.generate_content(prompt)
        # Clean up potential markdown or quotes from the response
        generated_text = response.text.strip().replace('"', '')

        # Create a Flask response object to properly handle text and headers
        resp = make_response(generated_text, 200)
        resp.headers.extend(headers)
        resp.headers['Content-Type'] = 'text/plain; charset=utf-8'
        return resp

    except Exception as e:
        error_message = f"Error generating content with Gemini API: {e}"
        print(error_message) # Log the error for debugging in Cloud Functions
        # Provide a fallback response
        fallback_text = "I... have nothing to say."
        resp = make_response(fallback_text, 500)
        resp.headers.extend(headers)
        return resp