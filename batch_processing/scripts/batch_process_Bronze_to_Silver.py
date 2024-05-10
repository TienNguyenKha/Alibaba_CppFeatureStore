import os
import pickle
import re
import sys
from io import BytesIO
from pathlib import Path
from typing import Optional, Union

import numpy as np
from minio import Minio
from pyspark import SparkContext
from pyspark.sql import SparkSession
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
    .config("spark.hadoop.fs.s3a.endpoint", f'{datalake_cfg["endpoint"]}')
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
    .appName("Python Spark transform Bronze to Silver")
    .getOrCreate()
)

sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
my_logger = log4jLogger.LogManager.getLogger("Preparing for Silver lake house")
my_logger.setLevel(log4jLogger.Level.WARN)
my_logger.info("Application is working well!")


# udf function:
def process_csv_line(col2):
    kv = np.array(re.split("[]", col2))
    keys = kv[range(0, len(kv), 3)]
    values = kv[range(1, len(kv), 3)]
    return dict(zip(keys.tolist(), values.tolist()))


def process_csv_line_2(col1, col2, col3, col5, value_dict):
    kv = np.array(re.split("[]", col5))
    key = kv[range(0, len(kv), 3)]
    value = kv[range(1, len(kv), 3)]
    feat_dict = dict(zip(key.tolist(), value.tolist()))
    feat_dict.update(value_dict)
    feat_dict["click"] = col1
    feat_dict["conversion"] = col2
    return feat_dict


def _convert_common_features_spark(spark: SparkSession, common_path, pickle_path=None):
    common = {}
    common_df = spark.read.option("delimiter", ",").csv(common_path)
    # common_df.repartition(13, "_c0")
    common_df = common_df.coalesce(13)
    custom_udf = udf(process_csv_line, MapType(StringType(), StringType()))
    df_result = (
        common_df.withColumn("Result", custom_udf(col("_c2")))
        .drop(col("_c1"))
        .drop(col("_c2"))
    )
    common = df_result.rdd.map(lambda row: (row["_c0"], row["Result"])).collectAsMap()
    if pickle_path:
        pickled_data = pickle.dumps(common, protocol=pickle.HIGHEST_PROTOCOL)
        pickled_data_io = BytesIO(pickled_data)
        client.put_object(
            bucket_name=datalake_cfg["bronze_bucket_name"],
            object_name=os.path.join(
                datalake_cfg["folder_common"], os.path.basename(pickle_path)
            ),
            data=pickled_data_io,
            length=len(pickled_data),
            content_type="application/octet-stream",
        )

    return common


def _convert_data_spark(
    spark: SparkSession,
    data_dir,
    file_size,
    is_train=True,
    output_dir=None,
):
    data_type = "train" if is_train else "test"
    output_dir = output_dir or os.path.join(
        "s3a://",
        datalake_cfg["silver_bucket_name"],
        datalake_cfg[f"silver_folder_name_{data_type}"],
    )

    common_path = os.path.join(data_dir, data_type, f"common_features_{data_type}.csv")
    skeleton_path = os.path.join(
        data_dir, data_type, f"sample_skeleton_{data_type}.csv"
    )
    common_features_path = os.path.join(f"{data_type}_common_features.pickle")
    global common
    common = {}

    try:
        client.stat_object(
            datalake_cfg["bronze_bucket_name"],
            os.path.join(
                datalake_cfg["folder_common"],
                os.path.basename(common_features_path),
            ),
        )
        my_logger.info(f"The {common_features_path} already exists in the bucket")
        # load object:
        response = client.get_object(
            datalake_cfg["bronze_bucket_name"],
            os.path.join(
                datalake_cfg["folder_common"],
                os.path.basename(common_features_path),
            ),
        )

        # Deserialize the pickled data
        common_pickled_data = response.read()
        common = pickle.loads(common_pickled_data)
        logger.info(
            f'The {common_features_path} already exists in bucket {datalake_cfg["bronze_bucket_name"]}, loaded successfully!'
        )
    except Exception as err:
        # my_logger.info(f"The {common_features_path} does not exist in the bucket")
        logger.info(f"The {common_features_path} does not exist in the bucket")

    if not common:
        # my_logger.info(f"Processing {data_type} common feature ...")
        logger.info(f"Processing {data_type} common feature ...")
        pickle_path = common_features_path
        common = _convert_common_features_spark(spark, common_path, pickle_path)

    for key, value in tqdm(common.items(), "Converting numpy type to string type ..."):
        common[key] = {str(key_): str(value_) for (key_, value_) in value.items()}

    # my_logger.info(f"Filtering and Transforming {data_type} dataframe ...")
    logger.info(f"Filtering and Transforming {data_type} dataframe ...")
    skeleton_df = spark.read.option("delimiter", ",").csv(skeleton_path).coalesce(13)

    common_df = spark.createDataFrame(common.items(), ["key", "value"])
    common_df = common_df.withColumn("str_key", col("key").cast("string")).drop(
        col("key")
    )
    joined_df = skeleton_df.join(
        common_df, skeleton_df._c3 == common_df.str_key, "left"
    )

    condition = (col("_c1") != "0") | (col("_c2") != "1")
    filtered_df = joined_df.filter(condition)
    custom_udf = udf(process_csv_line_2, MapType(StringType(), StringType()))
    df_processed = (
        filtered_df.withColumn(
            "Result",
            custom_udf(col("_c1"), col("_c2"), col("_c3"), col("_c5"), col("value")),
        )
        .drop(col("_c0"))
        .drop(col("_c1"))
        .drop(col("_c2"))
        .drop(col("_c3"))
        .drop(col("_c4"))
        .drop(col("_c5"))
        .drop(col("value"))
        .drop(col("str_key"))
    )

    # my_logger.info(f"Mapping {data_type} dataframe's ID:column_name ...")
    logger.info(f"Mapping {data_type} dataframe's ID:column_name ...")
    df_result = (
        df_processed.select(
            when(col("Result.101").isNotNull(), col("Result.101").cast(IntegerType()))
            .otherwise(None)
            .alias("user_id"),
            when(
                col("Result.109_14").isNotNull(),
                col("Result.109_14").cast(IntegerType()),
            )
            .otherwise(None)
            .alias("user_categories"),
            when(
                col("Result.110_14").isNotNull(),
                col("Result.110_14").cast(IntegerType()),
            )
            .otherwise(None)
            .alias("user_shops"),
            when(
                col("Result.127_14").isNotNull(),
                col("Result.127_14").cast(IntegerType()),
            )
            .otherwise(None)
            .alias("user_brands"),
            when(
                col("Result.150_14").isNotNull(),
                col("Result.150_14").cast(IntegerType()),
            )
            .otherwise(None)
            .alias("user_intentions"),
            when(col("Result.121").isNotNull(), col("Result.121").cast(IntegerType()))
            .otherwise(None)
            .alias("user_profile"),
            when(col("Result.122").isNotNull(), col("Result.122").cast(IntegerType()))
            .otherwise(None)
            .alias("user_group"),
            when(col("Result.124").isNotNull(), col("Result.124").cast(IntegerType()))
            .otherwise(None)
            .alias("user_gender"),
            when(col("Result.125").isNotNull(), col("Result.125").cast(IntegerType()))
            .otherwise(None)
            .alias("user_age"),
            when(col("Result.126").isNotNull(), col("Result.126").cast(IntegerType()))
            .otherwise(None)
            .alias("user_consumption_1"),
            when(col("Result.127").isNotNull(), col("Result.127").cast(IntegerType()))
            .otherwise(None)
            .alias("user_consumption_2"),
            when(col("Result.128").isNotNull(), col("Result.128").cast(IntegerType()))
            .otherwise(None)
            .alias("user_is_occupied"),
            when(col("Result.129").isNotNull(), col("Result.129").cast(IntegerType()))
            .otherwise(None)
            .alias("user_geography"),
            when(col("Result.205").isNotNull(), col("Result.205").cast(IntegerType()))
            .otherwise(None)
            .alias("item_id"),
            when(col("Result.206").isNotNull(), col("Result.206").cast(IntegerType()))
            .otherwise(None)
            .alias("item_category"),
            when(col("Result.207").isNotNull(), col("Result.207").cast(IntegerType()))
            .otherwise(None)
            .alias("item_shop"),
            when(col("Result.210").isNotNull(), col("Result.210").cast(IntegerType()))
            .otherwise(None)
            .alias("item_intention"),
            when(col("Result.216").isNotNull(), col("Result.216").cast(IntegerType()))
            .otherwise(None)
            .alias("item_brand"),
            when(col("Result.508").isNotNull(), col("Result.508").cast(IntegerType()))
            .otherwise(None)
            .alias("user_item_categories"),
            when(col("Result.509").isNotNull(), col("Result.509").cast(IntegerType()))
            .otherwise(None)
            .alias("user_item_shops"),
            when(col("Result.702").isNotNull(), col("Result.702").cast(IntegerType()))
            .otherwise(None)
            .alias("user_item_brands"),
            when(col("Result.853").isNotNull(), col("Result.853").cast(IntegerType()))
            .otherwise(None)
            .alias("user_item_intentions"),
            when(col("Result.301").isNotNull(), col("Result.301").cast(IntegerType()))
            .otherwise(None)
            .alias("position"),
            when(
                col("Result.click").isNotNull(), col("Result.click").cast(IntegerType())
            )
            .otherwise(None)
            .alias("click"),
            when(
                col("Result.conversion").isNotNull(),
                col("Result.conversion").cast(IntegerType()),
            )
            .otherwise(None)
            .alias("conversion"),
        )
        .withColumn("click", when(col("click").isNotNull(), col("click").cast("int")))
        .withColumn(
            "conversion",
            when(col("conversion").isNotNull(), col("conversion").cast("int")),
        )
    )

    # Calculate the number of partitions needed based on rows_per_file ~ file_size
    num_partitions = (df_result.count() // file_size) + 1

    # Repartition the DataFrame into smaller chunks
    df_result = df_result.repartition(num_partitions)

    # Write DataFrame to MinIO, ignore if delta talbe exists
    # my_logger.info(f"Writting {data_type} delta table to MinIO  ...")
    logger.info(f"Writting {data_type} delta table to MinIO  ...")
    df_result.write.format("delta").mode("ignore").save(output_dir)


def prepare_aliccp(
    spark: SparkSession,
    data_dir: Union[str, Path],
    convert_train: bool = True,
    convert_test: bool = True,
    file_size: int = 100000,
    output_dir: Optional[Union[str, Path]] = None,
):
    """
    Convert Ali-CPP data to parquet files.

    To download the raw the Ali-CCP training and test datasets visit
    [tianchi.aliyun.com](https://tianchi.aliyun.com/dataset/dataDetail?dataId=408#1).
    """

    if convert_train:
        # my_logger.info(f"Converting train csv file ...")
        logger.info(f"Converting train csv file ...")
        _convert_data_spark(
            spark,
            str(data_dir),
            file_size,
            is_train=True,
            output_dir=output_dir,
        )

    if convert_test:
        # my_logger.info(f"Converting test csv file ...")
        logger.info(f"Converting test csv file ...")
        _convert_data_spark(
            spark,
            str(data_dir),
            file_size,
            is_train=False,
            output_dir=output_dir,
        )

    return data_dir


# Make silver bucket:
# Create bucket if not exist.
found = client.bucket_exists(bucket_name=datalake_cfg["silver_bucket_name"])
if not found:
    client.make_bucket(bucket_name=datalake_cfg["silver_bucket_name"])
    logger.info(f'Bucket {datalake_cfg["silver_bucket_name"]} is created ...')
else:
    logger.info(
        f'Bucket {datalake_cfg["silver_bucket_name"]} already exists, skip creating!'
    )
# Let's ETL data raw from Bronze to Silver bucket.
prepare_aliccp(
    spark=spark, data_dir=f"s3a://{datalake_cfg['bronze_bucket_name']}/", convert_train=True, convert_test=True
)
