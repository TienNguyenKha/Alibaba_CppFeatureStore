import sys
sys.path.append("..")
from utils.helpers import load_cfg

from postgresql_client import PostgresSQLClient


CFG_FILE = "../config.yaml"
def main():
    cfg = load_cfg(CFG_FILE)
    postgre_config = cfg["Postgres_config"]
    pc = PostgresSQLClient(
        host=postgre_config["POSTGRES_HOST"],
        database=postgre_config["POSTGRES_DB"],
        user=postgre_config["POSTGRES_USER"],
        password=postgre_config["POSTGRES_PASSWORD"],
        port=postgre_config["POSTGRES_PORT"]
    )

    # Create alicpp table
    create_table_query = """
        CREATE TABLE IF NOT EXISTS alicpp (
            user_id_raw INT,
            item_id_raw INT,
            user_id INT,
            item_id INT,
            item_category INT,
            item_shop INT,
            item_brand INT,
            user_shops INT,
            user_profile INT,
            user_group INT,
            user_gender INT,
            user_age INT,
            user_consumption_2 INT,
            user_is_occupied INT,
            user_geography INT,
            user_intentions INT,
            user_brands INT,
            user_categories INT,
            click INT,
            created TIMESTAMP,
            datetime TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS alicppretrieval (
            user_id_raw INT,
            item_id_raw INT,
            user_id INT,
            item_id INT,
            item_category INT,
            item_shop INT,
            item_brand INT,
            user_shops INT,
            user_profile INT,
            user_group INT,
            user_gender INT,
            user_age INT,
            user_consumption_2 INT,
            user_is_occupied INT,
            user_geography INT,
            user_intentions INT,
            user_brands INT,
            user_categories INT,
            click INT,
            created TIMESTAMP,
            datetime TIMESTAMP
        );

    """
    try:
        pc.execute_query(create_table_query)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
