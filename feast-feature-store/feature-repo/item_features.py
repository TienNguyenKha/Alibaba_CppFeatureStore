from datetime import timedelta
from feast.types import Int32
from feast import Field, FeatureView
from entities import item
from data_sources import alicpp_stats_batch_source, item_feature_batch_source


item_features_view = FeatureView(
    name="item_features",
    entities=[item],
    ttl=timedelta(days=365),
    schema=[
        Field(name="item_category", dtype=Int32),
        Field(name="item_shop", dtype=Int32),
        Field(name="item_brand", dtype=Int32),
        Field(name="item_id_raw", dtype=Int32),
    ],
    online=True,
    source=item_feature_batch_source,
)