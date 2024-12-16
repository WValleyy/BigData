from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from pyspark.sql.functions import to_timestamp
from pyspark.sql.window import Window

import pyhdfs
import json

# Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("Stock Analysis") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("ProcessAndStorage") \
    .config("spark.mongodb.output.uri", "mongodb://admin:admin@mongodb:27017/yelp.storage") \
    .getOrCreate()


hdfs = pyhdfs.HdfsClient(hosts="namenode:9870", user_name="hdfs")
directory = '/data'
files = hdfs.listdir(directory)
print("Files in '{}':".format(directory), files)

review_schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])
tip_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True),
        StructField("compliment_count", IntegerType(), True)
    ])
# Function to create a DataFrame from a file's content
def create_df(file_path, schema):
    try:
        # Read the file content
        file_content = hdfs.open(file_path).read().decode('utf-8')
        # Convert JSON content to Python dictionary
        data = json.loads(file_content)
        # Create a DataFrame using the defined schema
        return spark.createDataFrame([data], schema)
    except Exception as e:
        print("Failed to read '{}': {}".format(file_path, e))
        return None

# # Create an empty DataFrame with the specified schema
# df = spark.createDataFrame([], schema)

# # Iterate over files and create DataFrame
# for file in files:
#     file_path = "{}/{}".format(directory, file)
#     file_df = create_dataframe_from_file(file_path)
#     if file_df:
#         file_df = file_df.withColumn("date", to_timestamp(file_df["date"], 'yyyy-MM-dd\'T\'HH:mm:ss'))
#         df = df.unionByName(file_df)

# from pyspark.sql import functions as F

# # Example of writing the basic_stats DataFrame to MongoDB
# A.write.format("mongo").mode("append").save()
# # Similarly for other DataFrames
# B.write.format("mongo").mode("append").save()


