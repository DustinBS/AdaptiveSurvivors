# reset_hdfs.ps1

# Resets HDFS data by stopping services, deleting bind-mounted data, and restarting.


Write-Host "--- HDFS Data Reset ---"

# Stop HDFS services
try {
    docker compose stop namenode datanode | Out-Null
    Write-Host "HDFS services stopped."
} catch {
    Write-Warning "Failed to stop Docker services. Error: $($_.Exception.Message)"; exit 1
}

$namenodeDataPath = '.\data\namenode'
$datanodeDataPath = '.\data\datanode'
# Delete HDFS data directories
Write-Host "Deleting HDFS data: '$namenodeDataPath', '$datanodeDataPath'..."
try {
    if (Test-Path $namenodeDataPath) { Remove-Item -Path $namenodeDataPath -Recurse -Force -ErrorAction Stop | Out-Null }
    else { Write-Host "'$namenodeDataPath' not found." }

    if (Test-Path $datanodeDataPath) { Remove-Item -Path $datanodeDataPath -Recurse -Force -ErrorAction Stop | Out-Null }
    else { Write-Host "'$datanodeDataPath' not found." }
    Write-Host "HDFS data directories processed."
} catch {
    Write-Error "Failed to delete directories. Error: $($_.Exception.Message)"; exit 1
}

# Restart all services
Write-Host "Starting all Docker services..."
try {
    docker compose up -d --build namenode datanode | Out-Null
    Write-Host "All Docker services started."
} catch {
    Write-Warning "Failed to start Docker services. Error: $($_.Exception.Message)"; exit 1
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

Write-Host "--- Submitting Kafka Connect HDFS Sink Connector Configuration ---"
try {
    # Attempt to delete the connector first (in case it exists from a previous run), then create it.
    try { Invoke-RestMethod -Uri http://localhost:8083/connectors/hdfs-sink-combined-events -Method Delete } catch {}
    Invoke-RestMethod -Uri http://localhost:8083/connectors -Method Post -ContentType 'Application/json' -Body (Get-Content -Raw -Path ./Backend/KafkaConnect/connectors/hdfs-sink-gameplay-events.json)
    Write-Host "HDFS Sink connector submitted."
} catch {
    Write-Error "Failed to create or update connector hdfs-sink-combined-events: $($_.Exception.Message)"
}

Write-Host "--- Creating HDFS directories for Kafka Connect ---"
docker exec namenode hdfs dfs -mkdir -p /topics /logs /feature_store/live /training_data /apps/spark
docker exec namenode hdfs dfs -chmod -R 777 /topics /logs /feature_store /training_data /apps/spark

Write-Host "HDFS directories created and permissions set."
Write-Host "--- HDFS Data Reset Complete ---"