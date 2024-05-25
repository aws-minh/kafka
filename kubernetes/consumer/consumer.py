from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("SalesDataProcessor") \
    .config("spark.driver.extraClassPath", '/driver/mssql-jdbc-12.6.2.jre8.jar;/driver/mssql-jdbc-12.6.2.jre11.jar') \
    .getOrCreate()

# Kafka credentials
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
sasl_plain_username = "2GQZHXZS45VQ7PJO"
sasl_plain_password = "ZCpmF1goGE4bQH7YaZnJnTzQQTzAyGKBr7aMciru8IozDwN6Z/OgnyXmjIQaA0L2"
# Define schema for the data
schema = StructType() \
    .add("customer_id", IntegerType()) \
    .add("day", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("quantity", IntegerType()) \
    .add("sales_item", StringType()) \
    .add("product", StringType()) \
    .add("category", StringType())

# Read streaming data from Kafka
sales_data_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("security.protocol", security_protocol) \
    .option("sasl.mechanism", sasl_mechanism) \
    .option("sasl.username", sasl_plain_username) \
    .option("sasl.password", sasl_plain_password) \
    .option("subscribe", "sale") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Define the connection string for Azure SQL Server
sql_server_connection_string = "jdbc:sqlserver://mn-datafactory.database.windows.net:1433;database=kafkaDb;user=adsql@mn-datafactory;password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Write streaming data to Azure SQL Server
query = sales_data_df \
    .writeStream \
    .foreachBatch(lambda df, epoch_id: df.write.jdbc(url=sql_server_connection_string, table="sales", mode="append")) \
    .start()

query.awaitTermination()
