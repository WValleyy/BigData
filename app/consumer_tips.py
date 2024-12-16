import sys, json, app.hdfs_write as hdfs_write, findspark

from confluent_kafka import Consumer, KafkaError

from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta
import openai
findspark.init()

KAFKA_TOPIC_NAME = "tips_topic"
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29093"



scala_version = '2.12'
spark_version = '3.5'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.1'
]


if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("KafkaStreamingTips")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .load()

    stream_df = stream_df.select(col("value").cast("string").alias("data"))
    inputStream = stream_df.selectExpr("CAST(data as STRING)")
    tip_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True),
        StructField("compliment_count", IntegerType(), True)
    ])
    inputStream = inputStream.select(from_json(col("data"), tip_schema).alias("tips"))
    def process_batch(batch_df, batch_id):
        realtimeDatas = batch_df.select("tips.*")
        for realtimeData in realtimeDatas.collect():
            # Convert Row to a dictionary
            row_dict = realtimeData.asDict()
            # row_dict['date'] = row_dict['date'].isoformat()
            json_string = json.dumps(row_dict)
            print(json_string)
            print("**")
            hdfs_write.write_to_hdfs(json_string, "yelp_tips")
        print(f"Batch processed {batch_id} done!")

    query = inputStream \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
