import feast
import constants
import pandas as pd

feature_store = feast.FeatureStore(repo_path=constants.REPO_PATH)
# print(feature_store.get_feature_view("user_features").entities)


user_features=[ "user_shops", 
                        "user_profile", 
                        "user_group", 
                        "user_gender", 
                        "user_age", 
                        "user_consumption_2", 
                        "user_is_occupied", 
                        "user_geography", 
                        "user_intentions", 
                        "user_brands", 
                        "user_categories", 
                        "user_id" ]
feature_refs = [
    ":".join(["user_features", feature_name]) for feature_name in user_features
]

result= feature_store.get_online_features(
    features=feature_refs,
    entity_rows=[{"user_id_raw": 266555},{"user_id_raw":444499}]
).to_df()
print(result[user_features])


# entity_df = pd.DataFrame.from_dict(
#     {
#         # entity's join key -> entity values
#         "user_id_raw": [185699],
#     }
# )
# entity_df["event_timestamp"] = pd.to_datetime("now", utc=False)

# training_df = feature_store.get_historical_features(
#     entity_df=entity_df,
#     features=[
#         "user_features:user_id",
#         "user_features:user_age",
#     ],
# ).to_df()
# print(training_df)
