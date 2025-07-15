# Terraform/variables.tf

variable "gcp_project_id" {
  description = "The GCP Project ID."
  type        = string
}

variable "gcp_region" {
  description = "The GCP region for resources."
  type        = string
}

variable "service_account_name" {
  description = "The name of the service account."
  type        = string
}

variable "gcs_bucket_name_suffix" {
  description = "The suffix for the GCS bucket name used for Spark jobs."
  type        = string
}

variable "gemini_api_key" {
  description = "The API key for the Google Gemini API."
  type        = string
  sensitive   = true # Prevents Terraform from showing this value in logs.
}