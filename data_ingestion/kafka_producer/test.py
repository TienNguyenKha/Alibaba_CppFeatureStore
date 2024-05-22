from datetime import datetime
import json
import pandas as pd
from bson import json_util


df = pd.read_csv('./data_sample.csv')
for _ in range(10):
    row = df.sample(1)
    record = row.to_dict(orient='records')[0]
    record["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    record["datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(json.dumps(record, default=json_util.default).encode("utf-8"))
    break



# record = {}
# # Make event one more year recent to simulate fresher data
# record["created"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# schema_path = "./avro_schemas/alicpp_schema.avsc"

# with open(schema_path, "r") as f:
#     parsed_schema = json.loads(f.read())

# for field in parsed_schema["fields"]:
#     if field["name"] == "click" or "conversion":
#         record[field["name"]] = np.random.randint(low=0, high=1)
#     elif field["name"] == 
#     if field["name"]:
#         record[field["name"]] = np.random.randint(low=0, high=10000)
# print(record)
