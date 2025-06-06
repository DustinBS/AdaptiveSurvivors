# AdaptiveSurvivors

A multiplayer survival game built with Unity and cloud backend.

## Prerequisites (for development)

- **Git**
- **Unity 6 LTS**
- **Docker Desktop**
- **Node.js v22.13.0 LTS**
- **JDK 8 LTS** [Adoptium link](https://adoptium.net/temurin/releases/?version=8&os=any&arch=any)
- **Maven 3.9.10** [Apache Maven link](https://maven.apache.org/download.cgi)

## Quick Start

### 1. Clone and Setup

The Unity dev environment is a submodule since it's pretty big.

```powershell
# Clone repository only
git clone https://github.com/DustinBS/AdaptiveSurvivors.git

# Initialize submodules (if you want the Unity project)
git submodule update --init --recursive
```

Or clone with submodules in one step:
```powershell
git clone --recurse-submodules https://github.com/DustinBS/AdaptiveSurvivors.git
```

### 2. Start Backend Services

```powershell
# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 3. Open Unity Project

You can direct Unity Hub to "Add" AdaptiveSurvivors/GameClient using Unity 6 LTS and it should work.

## Development Workflow

It's essentially just working with 1 git repo in another, e.g.

### Working with Unity (GameClient submodule)

```powershell
# Make changes in Unity, then:
cd GameClient
git add .
git commit -m "Your changes"
git push origin main

# Update parent repository reference
cd ..
git add GameClient
git commit -m "Update GameClient"
git push origin main
```

### Pull Latest Changes

```powershell
git pull origin main
git submodule update --recursive --remote
```

## Common Commands

```powershell
# Check submodule status
git submodule status

# Reset Docker environment
docker-compose down
docker-compose up -d
```

## Project Structure

```
AdaptiveSurvivors/
├── Backend/           # Docker containers
├── CloudFunctions/    # GCP Serverless functions
├── GameClient/        # Unity project (submodule)
└── docker-compose.yml, .env.example, other files
```

## Planned Architecture
```mermaid
graph TD
    %% Define Node Styles
    classDef process fill:#e1f5fe,stroke:#4fc3f7,stroke-width:2px;
    classDef datastore fill:#e8f5e9,stroke:#66bb6a,stroke-width:2px,shape:cylinder;
    classDef cloud fill:#fff3e0,stroke:#ffb74d,stroke-width:2px;
    classDef gcp_datastore fill:#fbe9e7,stroke:#ff8a65,stroke-width:2px,shape:cylinder;

    %% Main Actor
    UnityClient["Unity Game Client (C#)"]

    subgraph Local Development Environment
        direction LR

        subgraph Real-time Processing
            direction TB
            Kafka[("Apache Kafka Bus")]:::datastore
            Flink["Apache Flink Consumer"]:::process
        end

        subgraph Batch Ingestion & Processing
            direction TB
            Connect["Kafka Connect"]:::process
            HDFS[("HDFS Data Lake")]:::datastore
            Spark["Apache Spark Job"]:::process
        end
    end

    subgraph "Google Cloud Platform (GCP)"
        direction TB
        CloudFunc["Cloud Function (LLM NPC)"]:::cloud
        GeminiAPI["Gemini 1.5 Flash API"]:::cloud

        subgraph BigQuery Warehouse
            BQ_Raw[("BQ Staging: gameplay_events_raw")]:::gcp_datastore
            BQ_Hist[("BQ History: gameplay_events_history")]:::gcp_datastore
            BQ_LLM[("BQ Prompts: llm_npc_prompts")]:::gcp_datastore
        end
    end

    %% --- Define The Connections ---

    %% Real-time Gameplay & Adaptation Loop
    UnityClient -->|"Gameplay Events (JSON)"| Kafka;
    Kafka -->|"Topic: gameplay_events"| Flink;
    Flink -->|"Topic: adaptive_params"| Kafka;
    Kafka -->|"Real-time Adaptive Params"| UnityClient;

    %% Raw Data Lake Ingestion (Batch)
    Kafka -->|"Topics: gameplay_events & adaptive_params"| Connect;
    Connect -->|"Batched Writes"| HDFS;

    %% Background Data Processing to Cloud
    HDFS -->|"Round-End Batch Read"| Spark;
    Spark -->|"Loads Transformed Data"| BQ_Raw;
    BQ_Raw -->|"SQL ETL"| BQ_Hist;
    BQ_Hist -->|"SQL Aggregation"| BQ_LLM;

    %% On-Demand LLM NPC Commentary
    UnityClient -.->|"On-Dialogue HTTP Request"| CloudFunc;
    CloudFunc -->|"Reads Run Summary"| BQ_LLM;
    CloudFunc -->|"Calls LLM"| GeminiAPI;
    GeminiAPI -->|"Returns Dialogue"| CloudFunc;
    CloudFunc -.->|"Returns Dialogue (JSON)"| UnityClient;
```
