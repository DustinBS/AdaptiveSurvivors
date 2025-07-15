# Terraform/main.tf

# 1. Configure the Google Cloud provider.
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4.0"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# 2. Enable APIs that are shared across all features.
resource "google_project_service" "iam" {
  service                    = "iam.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "gcs" {
  service                    = "storage-component.googleapis.com"
  depends_on                 = [google_project_service.iam]
  disable_dependent_services = true
}

resource "google_project_service" "bigquery" {
  service                    = "bigquery.googleapis.com"
  depends_on                 = [google_project_service.iam]
  disable_dependent_services = true
}

# 3. Create the a shared service account for the entire game backend.
resource "google_service_account" "sa" {
  account_id   = var.service_account_name
  display_name = "Adaptive Survivors Service Account"
  depends_on   = [google_project_service.iam]
}

# 4. Create a key for the service account
resource "google_service_account_key" "sa_key" {
  service_account_id = google_service_account.sa.name
  depends_on         = [google_service_account.sa]
}

# 5. Save the generated key to the local filesystem
resource "local_file" "service_account_key_file" {
  content  = base64decode(google_service_account_key.sa_key.private_key)
  filename = "${path.module}/../gcp-credentials/service-account-key.json"
}

# --- Feature Modules ---

module "ethereal_seer" {
  source = "./modules/ethereal_seer"
  count = 1 # 0 to disable; 1 to enable
  depends_on = [
    google_project_service.bigquery,
    google_project_service.gcs
  ]

  gcp_project_id         = var.gcp_project_id
  gcp_region             = var.gcp_region
  gcs_bucket_name_suffix = var.gcs_bucket_name_suffix
  service_account_email  = google_service_account.sa.email
}

module "post_run_commentary" {
  source = "./modules/post_run_commentary"
  count = 0 # 0 to disable; 1 to enable
  depends_on = [
    google_project_service.gcs
  ]

  gcp_project_id = var.gcp_project_id
  gcp_region     = var.gcp_region
  gemini_api_key = var.gemini_api_key
}
