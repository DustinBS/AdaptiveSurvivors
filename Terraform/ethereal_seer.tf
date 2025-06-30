# Terraform/ethereal_seer.tf
# This file contains the GCP configuration for the Ethereal Seer feature.

# 1. GCS Bucket for Spark temporary data and the HDFS data lake sink.
resource "google_storage_bucket" "spark_temp_bucket" {
  name                        = "${var.gcp_project_id}${var.gcs_bucket_name_suffix}"
  location                    = var.gcp_region
  force_destroy               = true
  uniform_bucket_level_access = true

  # Automatically delete temporary objects after 1 day.
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
  depends_on = [google_project_service.gcs]
}

# 2. BigQuery Dataset for ML model training data.
resource "google_bigquery_dataset" "staging_dataset" {
  dataset_id                 = "gameplay_data_staging"
  location                   = var.gcp_region
  delete_contents_on_destroy = true
  default_table_expiration_ms = 86400000 # 1 day
  depends_on                  = [google_project_service.bigquery]
}

# --- IAM roles for the Service Account related to the Seer feature ---

# 3a. Role for managing objects in the Spark GCS bucket.
resource "google_storage_bucket_iam_member" "gcs_binding" {
  bucket = google_storage_bucket.spark_temp_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.sa.email}"
  depends_on = [
    google_storage_bucket.spark_temp_bucket,
    google_project_service.gcs
  ]
}

# 3b. Legacy role needed by some GCS connectors.
resource "google_storage_bucket_iam_member" "gcs_legacy_reader_binding" {
  bucket = google_storage_bucket.spark_temp_bucket.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.sa.email}"
  depends_on = [
    google_storage_bucket.spark_temp_bucket,
    google_project_service.gcs
  ]
}

# 3c. Roles for running BigQuery jobs and editing data.
resource "google_project_iam_member" "bq_job_user_binding" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.sa.email}"
  depends_on = [
    google_project_service.bigquery,
    google_service_account.sa,
    google_project_service.iam
  ]
}

resource "google_project_iam_member" "bq_data_editor_binding" {
  project = var.gcp_project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.sa.email}"
  depends_on = [
    google_project_service.bigquery,
    google_service_account.sa,
    google_project_service.iam
  ]
}