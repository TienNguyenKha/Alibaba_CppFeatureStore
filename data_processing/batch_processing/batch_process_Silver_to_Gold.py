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
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import col
from pathlib import Path
from typing import Union
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import LongType
import random


import warnings
warnings.filterwarnings('ignore')

## Load config
CFG_FILE = "../../config.yaml"
cfg = load_cfg(CFG_FILE)
# MinIO config
datalake_cfg = cfg["minIO_config"]

# Postgres config
Postgres_config = cfg["Postgres_config"]

spark = (
    SparkSession.builder.master("local[*]")
    .config(
        "spark.jars",
        "../jars/aws-java-sdk-bundle-1.12.262.jar,../jars/hadoop-aws-3.3.4.jar,../jars/postgresql-42.6.0.jar",
    )
    .config("spark.hadoop.fs.s3a.endpoint", f'http://{datalake_cfg["endpoint"]}')
    .config("spark.hadoop.fs.s3a.access.key", f'{datalake_cfg["access_key"]}')
    .config("spark.hadoop.fs.s3a.secret.key", f'{datalake_cfg["secret_key"]}')
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1")
    .config(
        "spark.jars.repositories",
        "https://maven-central.storage-download.googleapis.com/maven2/",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.executor.memory", "18g")
    .config("spark.driver.memory", "5g")
    .appName("Python Spark transform Silver to Gold")
    .getOrCreate()
)

sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
my_logger = log4jLogger.LogManager.getLogger("Preparing for Silver lake house")
my_logger.setLevel(log4jLogger.Level.WARN)
my_logger.info("Application is working well!")

# Define the UDF function
def generate_random_timestamp():
    start_date = 1711904400 # 0h 1/4/2024
    end_date = 1713546000 # 0h 20/4/2024
    random_timestamp = random.randint(start_date, end_date)
    return random_timestamp


def Silver_convert_data_spark(
    spark: SparkSession,
    data_dir,
    is_train=True,
):
    data_type = "train" if is_train else "test"

    # df_delta_path = os.path.join(
    #     data_dir, f'{data_type}_1M'
    # )# "1M" for mini version of data

    df_delta_path = os.path.join(
        data_dir, f'{data_type}'
    )# full data

    df = spark.read.format("delta").load(df_delta_path).coalesce(13)
    
    # select desired columns and drop NA
    filtered_df = df.select(df["user_id"],df["item_id"],df["item_category"],df["item_shop"],
                           df["item_brand"], df["user_shops"], df["user_profile"], df["user_group"],
                           df["user_gender"],df["user_age"],df["user_consumption_2"],df["user_is_occupied"],
                           df["user_geography"], df["user_intentions"], df["user_brands"], df["user_categories"],
                           df["click"]).na.drop()
    
    cat_cols= ["user_id","item_id","item_category","item_shop",
                    "item_brand", "user_shops", "user_profile", "user_group",
                    "user_gender","user_age","user_consumption_2","user_is_occupied",
                    "user_geography", "user_intentions", "user_brands", "user_categories"]

    #saving raw_id and add timestamp column
    filtered_df = filtered_df.withColumn("user_id_raw", col("user_id"))\
                             .withColumn("item_id_raw", col("item_id"))\
                            #  .withColumn("created", current_timestamp())\
                            #  .withColumn("datetime", current_timestamp())
    
    ## Uncomment this part, If you wanna do a fast train stage later
    # sampled_item_ids=filtered_df.select("item_id").distinct().limit(10000) # for simple training later, i limit the number of items is 10k
    # sampled_item_ids.createOrReplaceTempView('sampled_item_ids')
    # # Filter the original DataFrame based on the sampled item IDs
    # filtered_df = filtered_df.filter(expr("item_id in (select `item_id` from sampled_item_ids)"))

    ## Feature engineering
    #Categorify all categorical columns:
    if not os.path.isdir('../pipeline_transform_model'):
        indexers = []
        # Create StringIndexer instances for each column
        for column in cat_cols:
            indexer = StringIndexer(inputCol=column, outputCol=f"{column}_index",handleInvalid="keep")
            indexers.append(indexer)

        # Create a Pipeline with all StringIndexer instances
        pipeline = Pipeline(stages=indexers)

        # Fit the Pipeline to the DataFrame
        pipeline_model = pipeline.fit(filtered_df)
        pipeline_model.save('../pipeline_transform_model')
    else:
        # When you rerun this file, you just need load the old pipeline and remember comment feature engineer Part
        pipeline_model= PipelineModel.load('../pipeline_transform_model')
    
    # Transform the data:
    drop_cols= cat_cols 
    transformed_df = pipeline_model.transform(filtered_df).drop(*drop_cols)

    # Rename to the original columns' name iteratively
    for column in cat_cols:
        transformed_df = transformed_df.withColumnRenamed(f"{column}_index", column)

    transformed_df= transformed_df.withColumn("idx", F.monotonically_increasing_id())
    windowSpec = Window.orderBy("idx")
    transformed_df = transformed_df.withColumn("idx", F.row_number().over(windowSpec))
    ### ADD datetime collumns:
    # Add a monotonically increasing ID for each row
    transformed_df = transformed_df.withColumn("row_id", F.monotonically_increasing_id())

    # Create an temporary DataFrame that has size equal to transformed_df
    timestamps_spark_df = transformed_df.select("user_id_raw")
    
    # Register the UDF function
    random_timestamp_udf = F.udf(generate_random_timestamp, LongType())   
    timestamps_spark_df = timestamps_spark_df.withColumn("created", random_timestamp_udf())
    timestamps_spark_df = timestamps_spark_df.orderBy(col("created")).drop("user_id_raw").withColumn("idx", F.monotonically_increasing_id())
    timestamps_spark_df_with_index = timestamps_spark_df.withColumn("idx", F.row_number().over(windowSpec))

    # Combine the original DataFrame with the timestamps DataFrame
    transformed_df = transformed_df.join(
        timestamps_spark_df_with_index, on="idx"
    ).drop("idx")

    transformed_df = transformed_df.withColumn("created",F.from_unixtime("created"))\
                                   .withColumn("datetime", F.col("created"))

    # # emb_counts={}
    # # for column in cat_cols:
    # #     emb_counts[column]=transformed_df.select(column).distinct().count()
    # # print('full data vocab: ',emb_counts)# get vocabsize for each cols

    # ## For DLRM model training purpose:
    # #full data vocab: {'user_id': 179853, 'item_id': 1843639, 'item_category': 7900, 'item_shop': 446101, 'item_brand': 191992, 'user_shops': 82869, 'user_profile': 97, 'user_group': 13, 'user_gender': 2, 'user_age': 7, 'user_consumption_2': 3, 'user_is_occupied': 2, 'user_geography': 4, 
    # #'user_intentions': 26184, 'user_brands': 41164, 'user_categories': 5308}
    
    #export user feature
    user_df= transformed_df.select(
        transformed_df["user_shops"], 
        transformed_df["user_profile"], 
        transformed_df["user_group"], 
        transformed_df["user_gender"], 
        transformed_df["user_age"], 
        transformed_df["user_consumption_2"], 
        transformed_df["user_is_occupied"], 
        transformed_df["user_geography"], 
        transformed_df["user_intentions"], 
        transformed_df["user_brands"], 
        transformed_df["user_categories"], 
        transformed_df["user_id_raw"], 
        transformed_df["user_id"],
        transformed_df["created"],
        transformed_df["datetime"] ).dropDuplicates(["user_id"])
    
    #export item feature
    item_df= transformed_df.select(
    transformed_df["created"], 
    transformed_df["datetime"],
    transformed_df["item_category"], 
        transformed_df["item_shop"], 
        transformed_df["item_brand"], 
        transformed_df["item_id_raw"], 
        transformed_df["item_id"]).dropDuplicates(["item_id"])
    
    #write to offline store:
    mode = "overwrite"
    url = f'jdbc:postgresql://{Postgres_config["POSTGRES_HOST"]}:{Postgres_config["POSTGRES_PORT"]}/{Postgres_config["POSTGRES_DB"]}'
    properties = {"user": f'{Postgres_config["POSTGRES_USER"]}',"password": f'{Postgres_config["POSTGRES_PASSWORD"]}',"driver": "org.postgresql.Driver"}
    transformed_df.write.jdbc(url=url, table="alicpp", mode=mode, properties=properties)
    user_df.write.jdbc(url=url, table="userfeature", mode=mode, properties=properties)
    item_df.write.jdbc(url=url, table="itemfeature", mode=mode, properties=properties)
    #interacted table:
    clicked_1_df = transformed_df.filter(transformed_df["click"] == 1)
    clicked_1_df.write.jdbc(url=url, table="alicppretrieval", mode=mode, properties=properties)
    
    emb_counts_clicked_1_df={}
    for column in cat_cols:
        emb_counts_clicked_1_df[column]=clicked_1_df.select(column).distinct().count()
    print('clicked_1_df data vocab: ',emb_counts_clicked_1_df)# get vocabsize for each cols, for 2tower model training purpose
    print(clicked_1_df.count())

def ingest_to_gold(
    spark: SparkSession,
    data_dir: Union[str, Path],
    convert_train: bool = True,
    convert_test: bool = True,
):
    if convert_train:
        Silver_convert_data_spark(
            spark,
            str(data_dir),
            is_train=True)
    if convert_test:
        Silver_convert_data_spark(
            spark,
            str(data_dir),
            is_train=False)

ingest_to_gold(
    spark=spark, data_dir=f"s3a://{datalake_cfg['silver_bucket_name']}/", convert_train=True, convert_test=False
)
