# Making changes

If you change the schema of a message (e.g., add a `damage_type` field to `weapon_hit_event` within the `gameplay_events topic`), here is the chain of dependencies you would need to update:

1. Unity Client (`KafkaClient.cs`): producer. You would first update the C# class definition (e.g., GameplayEvent or its payload dictionary) to include the new field. This is the source of the change.

2. Apache Flink Job (`Backend/FlinkJobs/.../PlayerProfileAndAdaptiveParametersJob.scala`): consumes `gameplay_events` to generate `adaptive_params`. It needs to deserialize the incoming JSON. You must update the corresponding Java/Scala data class (POJO) in your Flink project to match the new schema from Unity, otherwise deserialization will fail or miss the new data.

3. Apache Spark Job (`Backend/SparkJobs/.../HdfsToBigQueryStagingJob.scala`): reads the raw JSON from HDFS. You must update the explicit schema definition (`gameplayEventSchema` in the Scala code) to include the new field (e.g., StructField("damage_type", StringType, nullable = true)). If you don't, the new field will be ignored during processing.

4. Google BigQuery Table: the destination table must also be updated. If your Spark job tries to write a new column that doesn't exist in the BigQuery table, the write operation will fail. You would need to run an `ALTER TABLE your_dataset.your_table ADD COLUMN damage_type STRING;` command in BigQuery first.