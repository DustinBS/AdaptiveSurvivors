# Terraform/outputs.tf

output "post_run_commentary_function_url" {
  description = "The static trigger URL for the post-run commentary function."
  value       = length(module.post_run_commentary) > 0 ? module.post_run_commentary[0].function_url : "Not Deployed"
}
