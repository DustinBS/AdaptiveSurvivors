# generate-gcp-key.ps1
#
# Generates a new key for the service account defined in '.env'.
# Run this after 'terraform apply' or after manual setup to get a valid key for the new service account.
#
# --- Configuration ---
# The local path where the key file will be stored. Change this if you change volume Spark master/worker binding.
$KEY_FILE_PATH = "gcp-credentials/service-account-key.json"

# The path to the environment file.
$ENV_FILE_PATH = ".env"

# --- Execution ---
try {
    Write-Host "--- Generating new GCP service account key..." -ForegroundColor Yellow

    # Find and parse the .env file to get required variables.
    if (-not (Test-Path $ENV_FILE_PATH)) {
        throw "'.env' file not found in the project root. Please ensure it exists."
    }
    $envContent = Get-Content $ENV_FILE_PATH

    # Get GCP_PROJECT_ID from .env
    $GCP_PROJECT_ID = ($envContent | Where-Object { $_ -match '^GCP_PROJECT_ID=' } | ForEach-Object { ($_ -split '=', 2)[1].Trim() })
    if ([string]::IsNullOrWhiteSpace($GCP_PROJECT_ID)) {
        throw "GCP_PROJECT_ID not found or is empty in '$ENV_FILE_PATH'."
    }

    # Get GCP_SPARK_SERVICE_ACCOUNT_NAME from .env
    $SERVICE_ACCOUNT_NAME = ($envContent | Where-Object { $_ -match '^GCP_SPARK_SERVICE_ACCOUNT_NAME=' } | ForEach-Object { ($_ -split '=', 2)[1].Trim() })
    if ([string]::IsNullOrWhiteSpace($SERVICE_ACCOUNT_NAME)) {
        throw "GCP_SPARK_SERVICE_ACCOUNT_NAME not found or is empty in '$ENV_FILE_PATH'."
    }

    # Construct the full email address for the service account.
    $saEmail = "${SERVICE_ACCOUNT_NAME}@${GCP_PROJECT_ID}.iam.gserviceaccount.com"

    # Ensure the target directory exists.
    New-Item -Path (Split-Path $KEY_FILE_PATH) -ItemType Directory -Force -ErrorAction SilentlyContinue | Out-Null

    Write-Host "Project ID: '$GCP_PROJECT_ID' (from .env)"
    Write-Host "Service Account: '$saEmail' (from .env)"
    Write-Host "Target Path: '$KEY_FILE_PATH'"

    # Execute the gcloud command to create the key.
    gcloud iam service-accounts keys create $KEY_FILE_PATH --iam-account=$saEmail --project=$GCP_PROJECT_ID

    Write-Host "`n--- SUCCESS ---" -ForegroundColor Green
    Write-Host "New key created successfully."
}
catch {
    Write-Host "`n--- FAILED ---" -ForegroundColor Red
    Write-Host "Failed to create key. Check that you have run 'gcloud auth login' and 'terraform apply'."
    Write-Host "Error details: $($_.Exception.Message)"
    exit 1
}
