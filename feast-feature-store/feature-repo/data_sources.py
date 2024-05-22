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


alicpp_stats_stream_source = KafkaSource(
    name="alicpp_stats_stream_source",
    kafka_bootstrap_servers="localhost:9092",
    topic="alicpp_stream_data",
    timestamp_field="created",
    batch_source=alicpp_stats_batch_source,
    message_format=JsonFormat(
        schema_json="user_id_raw integer,\
                     item_id_raw integer,\
                     user_id integer,\
                     item_id integer,\
                     item_category integer,\
                     item_shop integer,\
                     item_brand integer,\
                     user_shops integer,\
                     user_profile integer,\
                     user_group integer,\
                     user_gender integer,\
                     user_age integer,\
                     user_consumption_2 integer,\
                     user_is_occupied integer,\
                     user_geography integer,\
                     user_intentions integer,\
                     user_brands integer,\
                     user_categories integer,\
                     click integer,\
                     created timestamp,\
                     datetime timestamp,\
                     "
    ),
    watermark_delay_threshold=timedelta(minutes=1),
)
