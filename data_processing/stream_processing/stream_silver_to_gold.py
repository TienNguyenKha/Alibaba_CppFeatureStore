import os
import sys
sys.path.append("../..")
from utils.helpers import load_cfg
from utils.logging import logger
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def load_pipeline_model():
  """
  Loads the pipeline model from disk.
  """
  return PipelineModel.load("../pipeline_transform_model")

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars",
            "../jars/aws-java-sdk-bundle-1.12.262.jar,../jars/hadoop-aws-3.3.4.jar,../jars/postgresql-42.6.0.jar",
        )
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
        .config(
            "spark.jars.repositories",
            "https://maven-central.storage-download.googleapis.com/maven2/",
        )
        .appName("Python Spark Streaming Silver to Gold")
        .getOrCreate()
    )

    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    my_logger = log4jLogger.LogManager.getLogger("Preparing for Silver lake house")
    my_logger.setLevel(log4jLogger.Level.WARN)
    my_logger.info("Application is working well!")

    schema= StructType([
        StructField("user_id", DoubleType()),
        StructField("user_categories", DoubleType()),
        StructField("user_shops", DoubleType()),
        StructField("user_brands",  DoubleType()),
        StructField("user_intentions",  DoubleType()),
        StructField("user_profile",  DoubleType()),
        StructField("user_group",  DoubleType()),
        StructField("user_gender",  DoubleType()),
        StructField("user_age",  DoubleType()),
        StructField("user_consumption_1",  DoubleType()),
        StructField("user_consumption_2",  DoubleType()),
        StructField("user_is_occupied",  DoubleType()),
        StructField("user_geography",  DoubleType()),
        StructField("item_id",  DoubleType()),
        StructField("item_category",  DoubleType()),
        StructField("item_shop",  DoubleType()),
        StructField("item_intention",  DoubleType()),
        StructField("item_brand",  DoubleType()),
        StructField("user_item_categories",  DoubleType()),
        StructField("user_item_shops",  DoubleType()),
        StructField("user_item_brands",  DoubleType()),
        StructField("user_item_intentions",  DoubleType()),
        StructField("position",  DoubleType()),
        StructField("click",  DoubleType()),
        StructField("conversion",  DoubleType()),
        StructField("created",  StringType()),
        StructField("datetime",  StringType()),
        ])
    
    cat_cols= ["user_id","item_id","item_category","item_shop",
                    "item_brand", "user_shops", "user_profile", "user_group",
                    "user_gender","user_age","user_consumption_2","user_is_occupied",
                    "user_geography", "user_intentions", "user_brands", "user_categories"]

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "alicpp_records") \
        .option("enable.auto.commit", "false")\
        .option("startingOffsets", "earliest") \
        .load()
    parsed_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value")).select("value.*")
    formatted_datetime_df = parsed_df.withColumn("created", to_timestamp(col("created")))\
                                     .withColumn("datetime", to_timestamp(col("datetime")))
    formatted_datetime_df.printSchema()

    #transforming
    custom_udf = udf(load_pipeline_model, PipelineModel)
    transformed_df = formatted_datetime_df.withColumn("transformed_data", custom_udf().transform(formatted_datetime_df))
    
   

   
