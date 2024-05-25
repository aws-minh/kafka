from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from confluent_kafka import Producer
import time
import json
config = {"bootstrap.servers":"pkc-56d1g.eastus.azure.confluent.cloud:9092",
"security.protocol":"SASL_SSL",
"sasl.mechanisms":"PLAIN",
"sasl.username":"CRWLWGMCXLBJGX74",
"sasl.password":"WgKQebqREVoTQOjlXIAKEbHYdfy2ktyaVTc6v1tloRVZBOriyDrrvVNOd99V3W0W",
"session.timeout.ms":"45000"}
def generate_sample_data():
    # Generate sample data for 100 data points per day
    for _ in range(100):
        customer_id = 1000  # Sample customer ID
        day = time.strftime("%Y-%m-%d")  # Current day
        amount = round(random.uniform(10, 1000), 2)  # Random amount
        quantity = random.randint(1, 10)  # Random quantity
        sales_item = "SampleItem"  # Sample sales item
        product = "SampleProduct"  # Sample product
        category = "SampleCategory"  # Sample category
        yield {"customer_id": customer_id, "day": day, "amount": amount, "quantity": quantity,
               "sales_item": sales_item, "product": product, "category": category}

def send_to_kafka(partition):
    producer = Producer(config)
    for record in partition:
        producer.produce(topic, key=None, value=json.dumps(record))
    producer.flush()

if __name__ == "__main__":
    # Read Kafka producer configuration from client.properties
    topic = "sales"

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("SalesProducer") \
        .config("spark.jars.packages", "spark-sql-kafka-0-10_2.12-3.2.1.jar") \
        .config("kafka.bootstrap.servers", config["bootstrap.servers"]) \
        .config("kafka.security.protocol", config["security.protocol"]) \
        .config("kafka.sasl.mechanisms", config["sasl.mechanisms"]) \
        .config("kafka.sasl.username", config["sasl.username"]) \
        .config("kafka.sasl.password", config["sasl.password"]) \
        .config("kafka.session.timeout.ms", config["session.timeout.ms"])\
        .config("subscribe", topic) \
        .getOrCreate()

    sc = spark.sparkContext
    ssc = StreamingContext(sc, 10)  # 10 second batch interval

    # Generate sample data RDD
    sample_data = sc.parallelize(generate_sample_data())

    # Convert RDD to DataFrame
    sample_df = spark.read.json(sample_data)

    # Write DataFrame to Kafka
    sample_df.foreachPartition(send_to_kafka)

    # Start streaming context
    ssc.start()
    ssc.awaitTermination()
