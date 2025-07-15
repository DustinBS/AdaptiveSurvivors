# Terraform/modules/ethereal_seer/variables.tf

variable "gcp_project_id" {
  description = "The GCP Project ID."
  type        = string
}

variable "gcp_region" {
  description = "The GCP region for resources."
  type        = string
}

variable "gcs_bucket_name_suffix" {
  description = "The suffix for the GCS bucket name used for Spark jobs."
  type        = string
}

variable "service_account_email" {
  description = "The email of the shared service account."
  type        = string
}
