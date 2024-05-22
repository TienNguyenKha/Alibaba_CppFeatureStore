from datetime import timedelta
from feast.types import Int32
from feast import Field, FeatureView
from entities import user_raw

from data_sources import alicpp_stats_batch_source

user_features_view = FeatureView(
    name="user_features",
    entities=[user_raw],
    ttl=timedelta(days=365),
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
    online=True,
    source=alicpp_stats_batch_source
)