# AdaptiveSurvivors/start_backend.ps1

# This script automates the startup of Docker Compose services,
# Kafka topic creation, Flink job submission, and Kafka Connect setup.
# Run this script from the root of your AdaptiveSurvivors monorepo:
# PS C:\Unity\AdaptiveSurvivors> .\start_backend.ps1
# To reset HDFS data before starting, use:
# PS C:\Unity\AdaptiveSurvivors> .\start_backend.ps1 -ResetHDFS

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false, HelpMessage="Resets HDFS data by stopping namenode/datanode and deleting bind-mounted data before starting other services.")]
    [switch]$ResetHDFS
)

# Function to encapsulate HDFS reset logic
function Reset-HDFSData {
    Write-Host "--- HDFS Data Reset ---"

    # Stop HDFS services. Handles cases where they might not be running.
    try {
        docker compose stop namenode datanode | Out-Null
        Write-Host "HDFS services stopped."
    } catch {
        Write-Warning "Failed to stop Docker services during HDFS reset: $($_.Exception.Message)"; return $false
    }

    $namenodeDataPath = '.\data\namenode'
    $datanodeDataPath = '.\data\datanode'
    # Delete HDFS data directories.
    Write-Host "Deleting HDFS data: '$namenodeDataPath', '$datanodeDataPath'..."
    try {
        if (Test-Path $namenodeDataPath) { Remove-Item -Path $namenodeDataPath -Recurse -Force -ErrorAction Stop | Out-Null }
        else { Write-Host "'$namenodeDataPath' not found." }

        if (Test-Path $datanodeDataPath) { Remove-Item -Path $datanodeDataPath -Recurse -Force -ErrorAction Stop | Out-Null }
        else { Write-Host "'$datanodeDataPath' not found." }
        Write-Host "HDFS data directories processed."
    } catch {
        Write-Error "Failed to delete directories during HDFS reset: $($_.Exception.Message)"; return $false
    }

    Write-Host "--- HDFS Data Reset Complete. Services will be started by main script. ---"
    return $true
}

# Helper function to parse a .env file and load variables into the session
function Import-EnvFile {
    param(
        [string]$Path = ".env"
    )
    if (Test-Path $Path) {
        Get-Content $Path | ForEach-Object {
            $line = $_.Trim()
            if ($line -and !$line.StartsWith("#")) {
                $parts = $line.Split("=", 2)
                if ($parts.Length -eq 2) {
                    $key = $parts[0].Trim()
                    $value = $parts[1].Trim()
                    [System.Environment]::SetEnvironmentVariable($key, $value, "Process")
                    Write-Host "Loaded env var: $key"
                }
            }
        }
    } else {
        Write-Warning ".env file not found at path: $Path"
    }
}

# --- Main Script Logic ---
Import-EnvFile
if ($ResetHDFS) {
    Write-Host "--- Performing HDFS data reset as requested ---"
    # Execute HDFS reset. Exit on failure.
    if (-not (Reset-HDFSData)) {
        Write-Error "HDFS reset failed. Exiting."
        exit 1
    }
}

Write-Host "--- Stopping Any Docker Compose Services ---"
docker compose down -v
Write-Host "--- Starting Docker Compose Services ---"
docker compose up -d --build

# This runs in parallel while the Docker containers are starting up.
Write-Host "--- (Re)Building All Backend Job JARs (Common, Flink, Spark) ---"
Push-Location .\Backend\
mvn clean package
if ($LASTEXITCODE -ne 0) {
    Write-Error "Maven build failed. Exiting."
    Pop-Location
    docker compose down -v
    exit 1
}
Pop-Location
Write-Host "--- Maven build complete. Proceeding with service configuration. ---"

# Helper function to reduce repetitive code for health checks.
function Wait-For-Service {
    param(
        [string]$ServiceName,
        [int]$TimeoutSeconds = 120
    )
    Write-Host "--- Waiting for $ServiceName to be healthy (up to $TimeoutSeconds seconds) ---"
    $timeout = New-TimeSpan -Seconds $TimeoutSeconds
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed -lt $timeout) {
        $status = docker-compose ps $ServiceName | Select-String "healthy"
        if ($status) {
            Write-Host "$ServiceName is healthy."
            return $true
        }
        Write-Host "Waiting for $ServiceName..."
        Start-Sleep -Seconds 5
    }
    Write-Error "$ServiceName did not become healthy in time. Exiting."
    exit 1
}

# Wait for all core infrastructure services to be healthy before proceeding.
Wait-For-Service -ServiceName "zookeeper" -TimeoutSeconds 60
Wait-For-Service -ServiceName "kafka"
Wait-For-Service -ServiceName "namenode"
Wait-For-Service -ServiceName "jobmanager"
Wait-For-Service -ServiceName "kafka-connect"
Wait-For-Service -ServiceName "spark-master"
Wait-For-Service -ServiceName "seer-orchestrator"

# --- Configure running services now that they are healthy and JARs are built ---

Write-Host "--- Creating Kafka Topics (if they don't exist) ---"
$topicCreationSuccess = $false
$attempts = 0
$maxTopicAttempts = 10
while (-not $topicCreationSuccess -and $attempts -lt $maxTopicAttempts) {
    try {
        Write-Host "Attempting to create Kafka topics (Attempt $($attempts + 1)/$maxTopicAttempts)..."
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic gameplay_events --partitions 1 --replication-factor 1 --if-not-exists
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic adaptive_params --partitions 1 --replication-factor 1 --if-not-exists
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic seer_triggers --partitions 1 --replication-factor 1 --if-not-exists
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic seer_results --partitions 1 --replication-factor 1 --if-not-exists
        Write-Host "Kafka topics created successfully."
        $topicCreationSuccess = $true
    } catch {
        Write-Warning "Kafka not ready for topic creation yet. Retrying in 5 seconds..."
        Start-Sleep -Seconds 5
        $attempts++
    }
}
if (-not $topicCreationSuccess) {
    Write-Error "Failed to create Kafka topics after multiple attempts. Exiting."
    exit 1
}

Write-Host "--- Creating HDFS directories for Kafka Connect and Spark application JARs ---"
docker exec namenode hdfs dfs -mkdir -p /topics /logs /feature_store/live /training_data /apps/spark
docker exec namenode hdfs dfs -chmod -R 777 /topics /logs /feature_store /training_data /apps/spark
Write-Host "HDFS directories created and permissions set."

Write-Host "--- Submitting Kafka Connect HDFS Sink Connector Configuration ---"
try {
    # Attempt to delete the connector first (in case it exists from a previous run), then create it.
    try { Invoke-RestMethod -Uri http://localhost:8083/connectors/hdfs-sink-combined-events -Method Delete } catch {}
    Invoke-RestMethod -Uri http://localhost:8083/connectors -Method Post -ContentType 'Application/json' -Body (Get-Content -Raw -Path ./Backend/KafkaConnect/connectors/hdfs-sink-gameplay-events.json)
    Write-Host "HDFS Sink connector submitted."
} catch {
    Write-Error "Failed to create or update connector hdfs-sink-combined-events: $($_.Exception.Message)"
}

Write-Host "--- Cancelling and Resubmitting Flink Job ---"
$flinkJobName = "Unified Player Profile & Feature Job"
try {
    $jobId = (docker exec jobmanager flink list -r | Select-String -Pattern $flinkJobName | ForEach-Object { if ($_ -match '([0-9a-fA-F]{32})') { $Matches[1] } })
    if ($jobId) {
        Write-Host "Found and cancelling running Flink job '$flinkJobName' (ID: $jobId)..."
        docker exec jobmanager flink cancel $jobId
        Start-Sleep -Seconds 3 # Give Flink a moment to process the cancellation
    }
} catch { Write-Warning "Could not check or cancel Flink job: $($_.Exception.Message)" }
# Submit the new job by copying the JAR and running it.
docker cp .\Backend\FlinkJobs\target\AdaptiveSurvivorsFlinkJobs-1.0-SNAPSHOT.jar jobmanager:/tmp/AdaptiveSurvivorsFlinkJobs.jar
docker exec jobmanager flink run -d /tmp/AdaptiveSurvivorsFlinkJobs.jar
Write-Host "Flink job submitted."

Write-Host "--- Submitting Spark Jobs and Configs to Spark Master ---"
# 1. Copy the JAR, log4j.properties, and seed_data.yml into HDFS namenode container
docker cp .\Backend\SparkJobs\target\spark-batch-jobs-1.0-SNAPSHOT.jar namenode:/tmp/AdaptiveSurvivorsSparkJobs.jar
docker cp .\Backend\SparkJobs\src\main\resources\log4j.properties namenode:/tmp/log4j.properties
docker cp .\Backend\SparkJobs\src\main\resources\config\seed_data.yml namenode:/tmp/seed_data.yml

# 2. From the namenode container, copy into HDFS, making it cluster-accessible
docker exec namenode hdfs dfs -put -f /tmp/AdaptiveSurvivorsSparkJobs.jar /apps/spark/AdaptiveSurvivorsSparkJobs.jar
docker exec namenode hdfs dfs -put -f /tmp/log4j.properties /apps/spark/log4j.properties
docker exec namenode hdfs dfs -put -f /tmp/seed_data.yml /apps/spark/seed_data.yml

Write-Host "--- Launching Spark Streaming Job: BootstrapTriggerListenerJob ---"
docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit `
    --class com.adaptivesurvivors.spark.BootstrapTriggerListenerJob `
    --master spark://spark-master:7077 `
    --deploy-mode cluster `
    --driver-memory 512m `
    --executor-memory 512m `
    --total-executor-cores 1 `
    --files hdfs://namenode:9000/apps/spark/log4j.properties `
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" `
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" `
    hdfs://namenode:9000/apps/spark/AdaptiveSurvivorsSparkJobs.jar `
    --gcp-project-id=$env:GCP_PROJECT_ID `
    --gcs-temp-bucket=$env:GCS_TEMP_BUCKET

Write-Host "--- Launching Spark Streaming Job: EnrichAndPersistTrainingRecordJob ---"
docker exec -d spark-master /opt/bitnami/spark/bin/spark-submit `
    --class com.adaptivesurvivors.spark.EnrichAndPersistTrainingRecordJob `
    --master spark://spark-master:7077 `
    --deploy-mode cluster `
    --driver-memory 512m `
    --executor-memory 512m `
    --total-executor-cores 1 `
    --files hdfs://namenode:9000/apps/spark/log4j.properties `
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" `
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" `
    hdfs://namenode:9000/apps/spark/AdaptiveSurvivorsSparkJobs.jar

Write-Host "--- Backend Services Setup Complete ---"
Write-Host "You can now run your Unity game and perform actions."
Write-Host "Check Kafka consumer logs and Flink UI (http://localhost:8082) for data flow."
Write-Host "To debug Flink job errors, run: docker logs jobmanager"
Write-Host "To check HDFS: http://localhost:9870/explorer.html"
Write-Host "To check Kafka Connect: http://localhost:8083/connectors"
Write-Host "To check Spark workers: http://localhost:8080"
Write-Host "To check Python workers: http://seer-orchestrator:5001/health"
