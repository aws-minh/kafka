from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
# Function to read Kafka configuration from client.properties
# Required connection configs for Kafka producer, consumer, and admin
config = {"bootstrap.servers":"pkc-56d1g.eastus.azure.confluent.cloud:9092",
"security.protocol":"SASL_SSL",
"sasl.mechanisms":"PLAIN",
"sasl.username":"CRWLWGMCXLBJGX74",
"sasl.password":"WgKQebqREVoTQOjlXIAKEbHYdfy2ktyaVTc6v1tloRVZBOriyDrrvVNOd99V3W0W",
"session.timeout.ms":"45000"}
# Function to create Spark session
def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
        .config("spark.driver.extraClassPath", '/driver/mssql-jdbc-12.6.2.jre8.jar;/driver/mssql-jdbc-12.6.2.jre11.jar') \
        .getOrCreate()

# Kafka configuration
topic = "sales"

# Spark session
spark = create_spark_session()

# Schema for the incoming Kafka messages
schema = StructType() \
    .add("customer_id", IntegerType()) \
    .add("day", StringType()) \
    .add("amount", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("sales_item", StringType()) \
    .add("product", StringType()) \
    .add("category", StringType())

# Read data from Kafka into a DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", config["bootstrap.servers"]) \
    .option("kafka.security.protocol", config["security.protocol"]) \
    .option("kafka.sasl.mechanisms", config["sasl.mechanisms"]) \
    .option("kafka.sasl.username", config["sasl.username"]) \
    .option("kafka.sasl.password", config["sasl.password"]) \
    .option("kafka.session.timeout.ms", config["session.timeout.ms"])\
    .option("subscribe", topic) \
    .load()

# Deserialize JSON data from Kafka
parsed_df = kafka_df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Write the streaming DataFrame to SQL Server
query = parsed_df \
    .writeStream \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://mn-datafactory.database.windows.net:1433;database=kafkaDb;user=adsql@mn-datafactory;password=your_password_here;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;") \
    .option("dbtable", "kafka_data") \
    .option("user", "adsql@mn-datafactory") \
    .option("password", "Quynh2904") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .start()

# Await termination
query.awaitTermination()
