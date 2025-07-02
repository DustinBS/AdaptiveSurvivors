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

# --- Main Script Logic ---

if ($ResetHDFS) {
    Write-Host "--- Performing HDFS data reset as requested ---"
    # Execute HDFS reset. Exit on failure.
    if (-not (Reset-HDFSData)) {
        Write-Error "HDFS reset failed. Exiting."
        exit 1
    }
}

Write-Host "--- Starting Docker Compose Services ---"
docker-compose up -d --build

Write-Host "--- Waiting for Zookeeper to be healthy (up to 60 seconds) ---"
$timeout = New-TimeSpan -Seconds 60
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while ($sw.Elapsed -lt $timeout) {
    $status = docker-compose ps zookeeper | Select-String "healthy"
    if ($status) {
        Write-Host "Zookeeper is healthy."
        break
    }
    Write-Host "Waiting for Zookeeper..."
    Start-Sleep -Seconds 5
}
if ($sw.Elapsed -ge $timeout) {
    Write-Error "Zookeeper did not become healthy in time. Exiting."
    exit 1
}

Write-Host "--- Waiting for Kafka to be healthy (up to 120 seconds) ---"
$timeout = New-TimeSpan -Seconds 120
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while ($sw.Elapsed -lt $timeout) {
    $status = docker-compose ps kafka | Select-String "healthy"
    if ($status) {
        Write-Host "Kafka is healthy."
        # Add a small additional buffer after health check for Kafka broker to fully initialize
        Write-Host "Giving Kafka a few more seconds to warm up..."
        Start-Sleep -Seconds 10 # Added extra sleep
        break
    }
    Write-Host "Waiting for Kafka..."
    Start-Sleep -Seconds 5
}
if ($sw.Elapsed -ge $timeout) {
    Write-Error "Kafka did not become healthy in time. Exiting."
    exit 1
}

Write-Host "--- Waiting for Namenode to be healthy (up to 120 seconds) ---"
$timeout = New-TimeSpan -Seconds 120
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while ($sw.Elapsed -lt $timeout) {
    $status = docker-compose ps namenode | Select-String "healthy"
    if ($status) {
        Write-Host "Namenode is healthy."
        break
    }
    Write-Host "Waiting for Namenode..."
    Start-Sleep -Seconds 5
}
if ($sw.Elapsed -ge $timeout) {
    Write-Error "Namenode did not become healthy in time. Exiting."
    exit 1
}

Write-Host "--- Waiting for Kafka-Connect to be healthy (up to 120 seconds) ---"
$timeout = New-TimeSpan -Seconds 120
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while ($sw.Elapsed -lt $timeout) {
    $status = docker-compose ps kafka-connect | Select-String "healthy"
    if ($status) {
        Write-Host "Kafka-Connect is healthy."
        break
    }
    Write-Host "Waiting for Kafka-Connect..."
    Start-Sleep -Seconds 5
}
if ($sw.Elapsed -ge $timeout) {
    Write-Error "Kafka-Connect did not become healthy in time. Exiting."
    exit 1
}

Write-Host "--- Waiting for Flink JobManager to be healthy (up to 120 seconds) ---"
$timeout = New-TimeSpan -Seconds 120
$sw = [System.Diagnostics.Stopwatch]::StartNew()
while ($sw.Elapsed -lt $timeout) {
    $status = docker-compose ps jobmanager | Select-String "healthy"
    if ($status) {
        Write-Host "Flink JobManager is healthy."
        break
    }
    Write-Host "Waiting for Flink JobManager..."
    Start-Sleep -Seconds 5
}
if ($sw.Elapsed -ge $timeout) {
    Write-Error "Flink JobManager did not become healthy in time. Exiting."
    exit 1
}


Write-Host "--- Creating Kafka Topics (if they don't exist) ---"
# Loop to ensure Kafka topics can be created, indicating Kafka is truly ready for topics API
$topicCreationSuccess = $false
$attempts = 0
$maxTopicAttempts = 10
while (-not $topicCreationSuccess -and $attempts -lt $maxTopicAttempts) {
    try {
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic gameplay_events --partitions 1 --replication-factor 1 --if-not-exists 2>$null
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic adaptive_params --partitions 1 --replication-factor 1 --if-not-exists 2>$null
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic seer_encounter_trigger --partitions 1 --replication-factor 1 --if-not-exists 2>$null
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic bqml_features --partitions 1 --replication-factor 1 --if-not-exists 2>$null
        docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic seer_results --partitions 1 --replication-factor 1 --if-not-exists 2>$null
        Write-Host "Kafka topics created successfully."
        $topicCreationSuccess = $true
    } catch {
        Write-Host "Kafka not ready for topic creation yet. Retrying in 5 seconds..."
        Start-Sleep -Seconds 5
        $attempts++
    }
}
if (-not $topicCreationSuccess) {
    Write-Error "Failed to create Kafka topics after multiple attempts. Exiting."
    exit 1
}


Write-Host "--- (re)Building Flink Job JAR ---"
Push-Location .\Backend\FlinkJobs\
mvn clean package
Pop-Location

Write-Host "--- Cancelling and Resubmitting Flink Job ---"

$flinkJobName = "Adaptive Survivors Player Profile and Adaptive Parameters Job"
try {
    # Attempt to find and cancel the running job
    $jobList = docker exec jobmanager flink list -r -s
    $jobId = ($jobList | Select-String -Pattern ([regex]::Escape($flinkJobName)) | ForEach-Object {
        if ($_ -match '([0-9a-fA-F]{32})') { $Matches[1] }
    })

    if ($jobId) {
        Write-Host "Found and cancelling Flink job '$flinkJobName' (ID: $jobId)..."
        docker exec jobmanager flink cancel $jobId
        Start-Sleep -Seconds 10
    } else {
        Write-Host "No running job '$flinkJobName' found."
    }
} catch {
    Write-Warning "Failed to check or cancel Flink job: $($_.Exception.Message)"
}

# Copy and submit the new JAR
docker cp .\Backend\FlinkJobs\target\AdaptiveSurvivorsFlinkJobs-1.0-SNAPSHOT.jar jobmanager:/tmp/AdaptiveSurvivorsFlinkJobs.jar
docker exec jobmanager flink run -d /tmp/AdaptiveSurvivorsFlinkJobs.jar
Write-Host "Flink Job submitted."

Write-Host "--- Submitting Kafka Connect HDFS Sink Connector Configuration ---"
try {
    try { Invoke-RestMethod -Uri http://localhost:8083/connectors/hdfs-sink-combined-events -Method Delete } catch {}; Invoke-RestMethod -Uri http://localhost:8083/connectors -Method Post -ContentType 'Application/json' -Body (Get-Content -Raw -Path ./Backend/KafkaConnect/connectors/hdfs-sink-gameplay-events.json)
} catch {
    # This error is expected if the connector already exists, so we just log it as a warning
    Write-Warning "Connector hdfs-sink-gameplay-events might already exist or there was another issue: $($_.Exception.Message)"
}

Write-Host "--- Creating HDFS directories for Kafka Connect ---"
docker exec namenode hdfs dfs -mkdir -p /topics
docker exec namenode hdfs dfs -mkdir -p /logs
docker exec namenode hdfs dfs -chmod 777 /topics
docker exec namenode hdfs dfs -chmod 777 /logs

Write-Host "HDFS directories created and permissions set."

Write-Host "--- (re)Building Spark Batch Job ---"
Push-Location .\Backend\SparkJobs\
mvn clean package
Pop-Location
Write-Host "--- Submitting Spark Jobs and configs to Spark Master ---"
docker cp .\Backend\SparkJobs\target\spark-batch-jobs-1.0-SNAPSHOT.jar spark-master:/tmp/AdaptiveSurvivorsSparkJobs.jar
docker cp .\Backend\SparkJobs\src\main\resources\log4j.properties spark-master:/opt/bitnami/spark/conf/log4j.properties

Write-Host "--- Backend Services Setup Complete ---"
Write-Host "You can now run your Unity game and perform actions."
Write-Host "Check Kafka consumer logs and Flink UI (http://localhost:8081) for data flow."
Write-Host "To debug Flink job errors, run: docker logs jobmanager"
Write-Host "To check HDFS: http://localhost:9870/explorer.html"
Write-Host "To check Kafka Connect: http://localhost:8083/connectors"
Write-Host "To check Spark workers: http://localhost:8080"
