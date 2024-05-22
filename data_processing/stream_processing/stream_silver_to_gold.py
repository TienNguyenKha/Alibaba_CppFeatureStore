import sys
sys.path.append("../..")
from utils.helpers import load_cfg
from utils.logging import logger
from pyspark import SparkContext

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        # .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1")
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")
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

    pipeline_model= PipelineModel.load('../pipeline_transform_model')

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
    formatted_df = parsed_df.withColumn("user_id_raw", col("user_id"))\
                            .withColumn("item_id_raw", col("item_id"))\
                            #  .na.drop()
    
    # Rename the columns iteratively (to original name)
    transformed_df = pipeline_model.transform(formatted_df).drop(*cat_cols)
    for column in cat_cols:
        transformed_df = transformed_df.withColumnRenamed(f"{column}_index", column)

    changedTypedf = transformed_df.withColumn("user_id_raw", col("user_id_raw").cast(IntegerType()))\
                                  .withColumn("item_id_raw", col("item_id_raw").cast(IntegerType()))\
                                  .withColumn("user_id", col("user_id").cast(IntegerType()))\
                                  .withColumn("item_id", col("item_id").cast(IntegerType()))\
                                  .withColumn("item_category", col("item_category").cast(IntegerType()))\
                                  .withColumn("item_shop", col("item_shop").cast(IntegerType()))\
                                  .withColumn("item_brand", col("item_brand").cast(IntegerType()))\
                                  .withColumn("user_shops", col("user_shops").cast(IntegerType()))\
                                  .withColumn("user_profile", col("user_profile").cast(IntegerType()))\
                                  .withColumn("user_group", col("user_group").cast(IntegerType()))\
                                  .withColumn("user_gender", col("user_gender").cast(IntegerType()))\
                                  .withColumn("user_age", col("user_age").cast(IntegerType()))\
                                  .withColumn("user_consumption_2", col("user_consumption_2").cast(IntegerType()))\
                                  .withColumn("user_is_occupied", col("user_is_occupied").cast(IntegerType()))\
                                  .withColumn("user_geography", col("user_geography").cast(IntegerType()))\
                                  .withColumn("user_intentions", col("user_intentions").cast(IntegerType()))\
                                  .withColumn("user_brands", col("user_brands").cast(IntegerType()))\
                                  .withColumn("user_categories", col("user_categories").cast(IntegerType()))\
                                  .withColumn("click", col("click").cast(IntegerType()))\
                                  .withColumn("created", to_timestamp(col("created")))\
                                  .withColumn("datetime", to_timestamp(col("datetime")))\

    #serialize data    
    alicpp_df = changedTypedf.selectExpr( """to_json(named_struct(
                                            'user_id_raw', user_id_raw,
                                            'item_id_raw', item_id_raw,
                                            'user_id', user_id,
                                            'item_id', item_id,
                                            'item_category', item_category,
                                            'item_shop', item_shop,
                                            'item_brand', item_brand,
                                            'user_shops', user_shops,
                                            'user_profile', user_profile,
                                            'user_group', user_group,
                                            'user_gender', user_gender,
                                            'user_age', user_age,
                                            'user_consumption_2', user_consumption_2,
                                            'user_is_occupied', user_is_occupied,
                                            'user_geography', user_geography,
                                            'user_intentions', user_intentions,
                                            'user_brands', user_brands,
                                            'user_categories', user_categories,
                                            'click', click,
                                            'created', created,
                                            'datetime', datetime
                                           )) as value""")

    alicpp_df_writer_query = alicpp_df \
        .writeStream \
        .queryName("alicpp_df Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "alicpp_stream_data") \
        .outputMode("append") \
        .trigger(processingTime="10 second") \
        .partitionBy("user_group")\
        .option("checkpointLocation", "chk-point-dir/alicpp_df") \
        .start()
    
    
    logger.info("Listening and writing to Kafka")
    alicpp_df_writer_query.awaitTermination()

    
   

   
