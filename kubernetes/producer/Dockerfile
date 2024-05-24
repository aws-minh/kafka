# Use the official Spark base image
FROM apache/spark:3.5.1

# Install necessary dependencies
USER root

RUN apt-get update && \
    apt-get install -y curl apt-transport-https gnupg2 lsb-release && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    apt-get update && \
    apt install -y curl unixodbc odbcinst && \
    wget https://packages.microsoft.com/ubuntu/20.04/prod/pool/main/m/msodbcsql17/msodbcsql17_17.10.1.1-1_amd64.deb && \
    ACCEPT_EULA=Y dpkg --install msodbcsql17_17.10.1.1-1_amd64.deb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /opt/spark-apps

# Copy the Spark application script into the container
COPY spark_producer.py /opt/spark-apps/

# Add the necessary JAR files for Kafka
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.5.1/spark-streaming-kafka-0-10-assembly_2.12-3.5.1.jar /opt/spark/jars/

# Set the entry point to run the Spark application
ENTRYPOINT [ "/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", "/opt/spark-apps/spark_producer.py" ]
