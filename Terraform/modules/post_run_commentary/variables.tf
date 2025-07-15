# Terraform/modules/post_run_commentary/variables.tf

variable "gcp_project_id" {
  description = "The GCP Project ID."
  type        = string
}

variable "gcp_region" {
  description = "The GCP region for resources."
  type        = string
}

variable "gemini_api_key" {
  description = "The API key for the Google Gemini API."
  type        = string
  sensitive   = true
}
