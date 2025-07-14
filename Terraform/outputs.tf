# Terraform/outputs.tf

output "post_run_commentary_function_url" {
  description = "The static trigger URL for the post-run commentary function."
  # The try() function will attempt to access the function's URI.
  # If the resource doesn't exist in the plan (due to -target), it will
  # fail gracefully and return the second argument, "Not Deployed".
  value       = try(google_cloudfunctions2_function.post_run_commentary.service_config[0].uri, "Not Deployed")
}