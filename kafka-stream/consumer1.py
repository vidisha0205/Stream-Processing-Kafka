from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import json
import time


consumer = KafkaConsumer('wiki',
bootstrap_servers=['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('ascii')))

# op_file = open("output.json",'w')

# try:
#     for message in consumer:
#             json.dump(message.value['query']['recentchanges'][0],op_file)
#             op_file.write(','+"\n")
# except:
    
 

KAFKA_TOPIC_NAME = 'wiki'
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# CHECKPOINT_LOCATION = "LOCAL DIRECTORY LOCATION (FOR DEBUGGING PURPOSES)"
CHECKPOINT_LOCATION = "/Users/vidishaattili/Documents/KAFKA"


if __name__ == "__main__":

    # STEP 1 : creating spark session object

    spark = (
        SparkSession.builder.appName("Kafka Pyspark Streaming Learning")
        .master("local[*]")
        .config("spark.ui.port","4050")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # STEP 2 : reading a data stream from a kafka topic

    sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
    base_df.printSchema()

    # STEP 3 : Applying suitable schema

    sample_schema = (
        StructType()
        .add("type", StringType(), False)
        .add("ns", IntegerType(), False)
        .add("title", StringType(), False)
        .add("pageid", LongType(), False)
        .add("revid", LongType(), False)
        .add("old_revid",LongType(), False)
        .add("rcid", LongType(), False)
        .add("user", StringType(), False)
        .add("oldlen", LongType(), False)
        .add("newlen", LongType(), False)

    )

    info_dataframe = base_df.select(
        from_json(col("value"), sample_schema).alias("info"), "timestamp"
    )

    info_dataframe.printSchema()
    info_df_fin = info_dataframe.select("info.*", "timestamp")
    info_df_fin.printSchema()

    #q1
    type_df = info_df_fin.groupBy(
        window(info_df_fin.timestamp,"1 minutes"," 1 minutes"),
        info_df_fin.type)
    query_1 = type_df.count()
   


    result = (
            query_1
            .writeStream
            .outputMode("update")
            .format("console")
            .start()
            .awaitTermination()
        )
   


    # result.awaitTermination()
