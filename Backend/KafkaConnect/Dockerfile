# Backend/KafkaConnect/Dockerfile
FROM confluentinc/cp-kafka-connect:7.0.1

# Install the HDFS Sink Connector from Confluent Hub.
# We are using version 10.2.15 of the HDFS Sink Connector, which is compatible
# with Confluent Platform 7.0.1 
# (HDFS Sink Connector 3 is a paid-license connector that I will not use)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:10.2.15

# Typically set by base image, but we are being explicit here
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components" 
