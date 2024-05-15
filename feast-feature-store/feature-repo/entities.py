from feast import Entity, ValueType

user_raw = Entity(name="user_id_raw", value_type=ValueType.INT32, description="user id raw")
item = Entity(name="item_id", value_type=ValueType.INT32, description="item id")

