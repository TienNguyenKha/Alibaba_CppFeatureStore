import os

import constants
import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import SparkProcessorConfig
from feast.infra.contrib.stream_processor import get_stream_processor_object
from pyspark.sql import SparkSession


os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell"
spark = SparkSession.builder.master("local").appName("feast-spark").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 5)

# Initialize the feature store
store = FeatureStore(repo_path=constants.REPO_PATH)


# If we want to further process features
# along with Spark, define it here
def preprocess_fn(rows: pd.DataFrame):
    print(f"[INFO] df columns: {rows.columns}")
    print(f"[INFO] df size: {rows.size}")
    print(f"[INFO]df preview:\n{rows.head()}")
    return rows


# Define ingestion config
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time="30 seconds",
    query_timeout=15,
)


# Initialize the item stream view from our feature store
item_sfv = store.get_stream_feature_view("item_stream")
# Initialize the processor
item_processor = get_stream_processor_object(
    config=ingestion_config,
    fs=store,
    sfv=item_sfv,
    preprocess_fn=preprocess_fn,
)
# Start to ingest
item_query = item_processor.ingest_stream_feature_view()

# You can push feature to offline store via the following commands
# item_query = item_processor.ingest_stream_feature_view(PushMode.OFFLINE)

# , and use this below command to stop ingestion job
# item_query.stop()


# Initialize the user stream view from our feature store
user_sfv = store.get_stream_feature_view("user_stream")
# Initialize the processor
user_processor = get_stream_processor_object(
    config=ingestion_config,
    fs=store,
    sfv=user_sfv,
    preprocess_fn=preprocess_fn,
)
# Start to ingest
user_query = user_processor.ingest_stream_feature_view()

# You can push feature to offline store via the following commands
# user_query = user_processor.ingest_stream_feature_view(PushMode.OFFLINE)

# , and use this below command to stop ingestion job
# user_query.stop()
