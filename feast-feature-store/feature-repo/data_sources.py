# This is an example feature definition file

from datetime import timedelta

from feast import KafkaSource
from feast.data_format import JsonFormat
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import \
    PostgreSQLSource

alicpp_stats_batch_source = PostgreSQLSource(
    name="alicpp",
    query="SELECT * FROM alicpp",
    timestamp_field="datetime",
    created_timestamp_column="created",
)

user_feature_batch_source = PostgreSQLSource(
    name="userfeature",
    query="SELECT * FROM userfeature",
    timestamp_field="datetime"
)

item_feature_batch_source = PostgreSQLSource(
    name="itemfeature",
    query="SELECT * FROM itemfeature",
    timestamp_field="datetime"
)

# device_stats_stream_source = KafkaSource(
#     name="device_stats_stream_source",
#     kafka_bootstrap_servers="localhost:9092",
#     topic="device_0",
#     timestamp_field="created",
#     batch_source=device_stats_batch_source,
#     message_format=JsonFormat(
#         schema_json="created timestamp, device_id integer, feature_5 double, feature_3 double, feature_1 double, feature_8 double, feature_6 double, feature_0 double, feature_4 double"
#     ),
#     watermark_delay_threshold=timedelta(minutes=1),
# )
