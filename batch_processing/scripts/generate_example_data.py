import os
import pickle
import re
import sys
from io import BytesIO
from pathlib import Path
from typing import Optional, Union

import numpy as np
from minio import Minio
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import IntegerType, MapType, StringType
from tqdm import tqdm

sys.path.append("../..")
from utils.helpers import load_cfg
from utils.logging import logger

# MinIO config
CFG_FILE = "../../config.yaml"
cfg = load_cfg(CFG_FILE)
datalake_cfg = cfg["minIO_config"]

#create miniIO client
client = Minio(
    endpoint=datalake_cfg["endpoint"],
    access_key=datalake_cfg["access_key"],
    secret_key=datalake_cfg["secret_key"],
    secure=False,
)

spark = (
    SparkSession.builder.master("local[*]")
    .config(
        "spark.jars",
        "../jars/aws-java-sdk-bundle-1.12.262.jar,../jars/hadoop-aws-3.3.4.jar",
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
    .appName("Python Spark generate test data for kafka streaming")
    .getOrCreate()
)

sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
my_logger = log4jLogger.LogManager.getLogger("Preparing for Silver lake house")
my_logger.setLevel(log4jLogger.Level.WARN)
my_logger.info("Application is working well!")

df_delta_path = "s3a://silver-alicpp/test"
df = spark.read.format("delta").load(df_delta_path).coalesce(13)
# fraction = 10000 / df.count()  # Adjust the fraction as needed
# random_df = df.sample(withReplacement=False, fraction=fraction)
df.limit(10000).coalesce(1).write.option("header",True) \
 .csv("../../data_ingestion/kafka_producer/kafka_records")