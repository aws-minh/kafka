from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, BooleanType
import time
import random

# Define the schema for the data
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("timestamp", LongType(), True),
    StructField("location", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("is_fraud", BooleanType(), True)
])

# Function to generate random transaction data
def generate_transaction(transaction_id):
    locations = ["New York, USA", "San Francisco, USA", "London, UK", "Tokyo, Japan", "Sydney, Australia"]
    transaction_types = ["purchase", "withdrawal", "transfer"]
    
    transaction = {
        "transaction_id": str(transaction_id),
        "user_id": str(random.randint(100, 200)),
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "timestamp": int(time.time()),
        "location": random.choice(locations),
        "transaction_type": random.choice(transaction_types),
        "is_fraud": random.choice([True, False])
    }
    return transaction

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkProducer") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = 'pkc-56d1g.eastus.azure.confluent.cloud:9092'
kafka_topic = 'transaction'
kafka_security_protocol = 'SASL_SSL'
kafka_sasl_mechanism = 'PLAIN'
kafka_sasl_username = 'ZF5RIE7TV7JPFMG7'
kafka_sasl_password = 'kY+KB8356Ahsj0H3/bBHGZjRtYUE54ymKaw1MvF3Ppz3ydHztxeuuIOnP55/xW4V'

# Generate data and write to Kafka
for i in range(1000):
    transaction = [generate_transaction(i)]
    df = spark.createDataFrame(transaction, schema)
    df.selectExpr("CAST(transaction_id AS STRING) AS key", "to_json(struct(*)) AS value") \
      .write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
      .option("topic", kafka_topic) \
      .option("kafka.security.protocol", kafka_security_protocol) \
      .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
      .option("kafka.sasl.username", kafka_sasl_username) \
      .option("kafka.sasl.password", kafka_sasl_password) \
      .save()

    time.sleep(3600 / 10)  # Sleep to produce 10 data points every hour

spark.stop()
