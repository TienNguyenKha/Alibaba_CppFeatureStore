## Create data schema
After putting your files to `MinIO``, please execute `trino` container by the following command:
```shell
docker exec -ti datalake-trino bash
```

When you are already inside the `trino` container, typing `trino` to in an interactive mode

After that, run the following command to register a new schema for our data:

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.alicpp
WITH (location = 's3://silver-alicpp/');

CREATE TABLE IF NOT EXISTS lakehouse.alicpp.train (
  user_id INT,
  user_categories INT,
  user_shops  INT,
  user_brands INT
) WITH (
  location = 's3://silver-alicpp/train'
);
```

## Query with DBeaver
1. Install `DBeaver` as in the following [guide](https://dbeaver.io/download/)
2. Connect to our database (type `trino`) using the following information (empty `password`):
  ![DBeaver Trino](./imgs/trino.png)