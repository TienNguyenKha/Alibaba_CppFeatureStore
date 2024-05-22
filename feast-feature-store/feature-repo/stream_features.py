from datetime import timedelta

from data_sources import alicpp_stats_stream_source
from entities import item, user_raw
from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Int32
from pyspark.sql import DataFrame

## stream item feature view
@stream_feature_view(
    entities=[item],
    ttl=timedelta(days=36500),
    mode="spark",
    schema=[
        Field(name="item_category", dtype=Int32),
        Field(name="item_shop", dtype=Int32),
        Field(name="item_brand", dtype=Int32),
        Field(name="item_id_raw", dtype=Int32),
    ],
    timestamp_field="created",
    online=True,
    source=alicpp_stats_stream_source,
)
def item_stream(df: DataFrame):
    return (
        df
    )

## stream user feature view
@stream_feature_view(
    entities=[user_raw],
    ttl=timedelta(days=36500),
    mode="spark",
    schema=[
        Field(name="user_shops", dtype=Int32),
        Field(name="user_profile", dtype=Int32),
        Field(name="user_group", dtype=Int32),
        Field(name="user_gender", dtype=Int32),
        Field(name="user_age", dtype=Int32),
        Field(name="user_consumption_2", dtype=Int32),
        Field(name="user_is_occupied", dtype=Int32),
        Field(name="user_geography", dtype=Int32),
        Field(name="user_intentions", dtype=Int32),
        Field(name="user_brands", dtype=Int32),
        Field(name="user_categories", dtype=Int32),
        Field(name="user_id", dtype=Int32),
    ],
    timestamp_field="created",
    online=True,
    source=alicpp_stats_stream_source,
)
def user_stream(df: DataFrame):
    return (
        df
    )