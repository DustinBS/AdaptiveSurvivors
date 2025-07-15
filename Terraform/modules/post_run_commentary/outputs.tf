# Terraform/modules/post_run_commentary/outputs.tf

output "function_url" {
  description = "The static trigger URL for the post-run commentary function."
  value       = google_cloudfunctions2_function.post_run_commentary.service_config[0].uri
}
