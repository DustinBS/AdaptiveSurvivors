# Terraform/main.tf

# 1. Configure the Google Cloud provider.
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    local = {
      source = "hashicorp/local"
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

# 3. Create the one shared service account for the entire game backend.
resource "google_service_account" "sa" {
  account_id   = var.service_account_name
  display_name = "Adaptive Survivors Service Account"
  depends_on   = [google_project_service.iam]
}

# 4. Create a key for the service account
resource "google_service_account_key" "sa_key" {
  service_account_id = google_service_account.sa.name
  depends_on = [google_service_account.sa]
}

# 5. Save the generated key to the local filesystem
resource "local_file" "service_account_key_file" {
  # The key content from the resource is base64 encoded, so we must decode it.
  content  = base64decode(google_service_account_key.sa_key.private_key)

  # This path navigates from the Terraform directory to your credentials directory.
  filename = "${path.module}/../gcp-credentials/service-account-key.json"
}
