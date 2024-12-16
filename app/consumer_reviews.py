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

KAFKA_TOPIC_NAME = "reviews_topic"
KAFKA_BOOTSTRAP_SERVERS = "kafka1:29092,kafka2:29093"



scala_version = '2.12'
spark_version = '3.5'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.1'
]



def sentiment_analysis(comment) -> str:
    if comment:
        openai.api_key = config['openai']['api_key']
        completion = openai.ChatCompletion.create(
            model='gpt-3.5-turbo',
            messages = [
                {
                    "role": "system",
                    "content": """
                        You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                        You are to respond with one word from the option specified above, do not add anything else.
                        Here is the comment:
                        
                        {comment}
                    """.format(comment=comment)
                }
            ]
        )
        return completion.choices[0].message['content']
    return "Empty"

if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("KafkaStreamingReviews")
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
    schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])
    inputStream = inputStream.select(from_json(col("data"), schema).alias("reviews"))
    stream_df = inputStream.select(("reviews.*"))

    sentiment_analysis_udf = udf(sentiment_analysis, StringType())

    stream_df = stream_df.withColumn('feedback',
                                     when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                     .otherwise(None)
                                     )

    
    

    kafka_df = stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value")
    # query = kafka_df.writeStream.outputMode("append").format("console").options(truncate=False).start()

    # query.awaitTermination()
    
    query = (kafka_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
            .option("kafka.security.protocol", config['kafka']['security.protocol'])
            .option("kafka.sasl.mechanism", config['kafka']['sasl.mechanisms'])
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config["kafka"]["sasl.username"]}" password="{config["kafka"]["sasl.password"]}";')
            .option("checkpointLocation", "/tmp/checkpoint")
            .option("topic", KAFKA_TOPIC_NAME)
            .start())
    
    # def process_batch(batch_df, batch_id):
    #     realtimeDatas = batch_df.select("reviews.*")
    #     for realtimeData in realtimeDatas.collect():
    #         # Convert Row to a dictionary
    #         row_dict = realtimeData.asDict()
    #         # row_dict['date'] = row_dict['date'].isoformat()
    #         json_string = json.dumps(row_dict)
    #         print(json_string)
    #         print("----------------------")
    #         hdfs_write.write_to_hdfs(json_string)
    #     print(f"Batch processed {batch_id} done!")

    # query = inputStream \
    #     .writeStream \
    #     .foreachBatch(process_batch) \
    #     .outputMode("append") \
    #     .start()

    query.awaitTermination()
    