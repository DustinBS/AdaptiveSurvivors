# Terraform/post_run_commentary.tf
# This file contains the GCP configuration for the post-run commentary function.

# 1. Enable APIs.
resource "google_project_service" "cloudfunctions_api" {
  service = "cloudfunctions.googleapis.com"
}
resource "google_project_service" "cloudbuild_api" {
  service = "cloudbuild.googleapis.com"
}
resource "google_project_service" "run_api" {
  service = "run.googleapis.com"
}

# 2. Create a GCS bucket to store the function's source code.
resource "google_storage_bucket" "commentary_function_source" {
  name     = "${var.gcp_project_id}-commentary-source"
  location = var.gcp_region
  force_destroy = true
}

# 3. Zip the source code from the CloudFunctions directory.
data "archive_file" "commentary_function_zip" {
  type        = "zip"
  source_dir  = "../CloudFunctions/post_run_commentary_function/"
  output_path = "post_run_commentary.zip"
}

# 4. Upload the zipped source code to the GCS bucket.
resource "google_storage_bucket_object" "commentary_function_source_upload" {
  name   = "source.zip"
  bucket = google_storage_bucket.commentary_function_source.name
  source = data.archive_file.commentary_function_zip.output_path
}

# 5. Define the Gen 2 Cloud Function resource.
resource "google_cloudfunctions2_function" "post_run_commentary" {
  name     = "post-run-commentary-function"
  location = var.gcp_region

  build_config {
    runtime     = "python311"
    entry_point = "post_run_commentary_function"
    source {
      storage_source {
        bucket = google_storage_bucket.commentary_function_source.name
        object = google_storage_bucket_object.commentary_function_source_upload.name
      }
    }
  }

  service_config {
    max_instance_count = 1
    min_instance_count = 0
    available_memory   = "256Mi"
    timeout_seconds    = 60
    environment_variables = {
      GEMINI_API_KEY = var.gemini_api_key
    }
  }

  depends_on = [
    google_project_service.cloudfunctions_api,
    google_project_service.cloudbuild_api
  ]
}

# 6. Make the function publicly accessible to get a static URL.
resource "google_cloud_run_service_iam_member" "commentary_public_access" {
  location = google_cloudfunctions2_function.post_run_commentary.location
  project  = google_cloudfunctions2_function.post_run_commentary.project
  service  = google_cloudfunctions2_function.post_run_commentary.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}