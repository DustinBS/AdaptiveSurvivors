# Terraform/outputs.tf

output "post_run_commentary_function_url" {
  description = "The static trigger URL for the post-run commentary function."
  value       = try(module.post_run_commentary.function_url, "Not Deployed")
}
