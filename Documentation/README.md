Integration tests
HDFS -> Spark
``` powershell
docker exec -it spark-master /opt/bitnami/spark/bin/pyspark

# Wait a bit, then run the following in the PySpark shell
# Define the path to your data in HDFS (* syntax works or you can target the .json directly)
hdfs_path = "hdfs://namenode:9000/topics/adaptive_params/2025-06-07/*"

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
