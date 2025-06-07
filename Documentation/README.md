Integration tests
Unity -> Kafka
``` powershell
docker exec kafka /bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic gameplay_events --from-beginning
# Watch for incoming messages to "gameplay_events" topic in Kafka
# Expect line-by-line json like
# {"event_type":"weapon_hit_event","timestamp":1749328373111,"player_id":"player_001","payload":{"weapon_id":"player_auto_attack","dmg_dealt":10.0,"enemy_id":"goblin_001"}}
# {"ev...
```

Flink -> Kafka
``` powershell
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic adaptive_params --from-beginning
# Watch for incoming messages to "adaptive_params" topic in Kafka
# Expect line-by-line json like
# {"playerId":"player_001","enemyResistances":{},"eliteBehaviorShift":"none","eliteStatusImmunities":[],"breakableObjectBuffsDebuffs":{},"timestamp":1749328374173}
# {"play...
```

HDFS 2 Sink Connector -> HDFS
The repo uses a version of namenode that has a Web UI file browser on http://localhost:9870/explorer.html#/. You should see `topics/adaptive_params`, `topics/gameplay_events`, and similar `logs/` there after the buffer flushes, otherwise it will be in the `topics/+tmp` folder. Alternatively, use the `hdfs dfs` script:
``` powershell
docker exec namenode hdfs dfs -ls /topics/adaptive_params/
```

HDFS -> Spark
``` powershell
# Start PySpark shell and wait for startup
docker exec -it spark-master /opt/bitnami/spark/bin/pyspark
```
``` powershell
# Define the path to your data in HDFS (* syntax works but it will be all files or you can target the .json directly)
hdfs_path = "hdfs://namenode:9000/topics/adaptive_params/*/*"

# Read the JSON files into a Spark DataFrame
# Spark will automatically infer the schema from the JSON structure
gameplay_df = spark.read.json(hdfs_path)

# Show the schema that Spark inferred
gameplay_df.printSchema()

# Show the first 20 rows of the DataFrame in a table format
gameplay_df.show()

# To exit the pyspark shell, type exit()
# exit()
```
