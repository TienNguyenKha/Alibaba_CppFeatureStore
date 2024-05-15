import os

from dotenv import load_dotenv
from postgresql_client import PostgresSQLClient

load_dotenv()


def main():
    pc = PostgresSQLClient(
        host="34.65.156.139",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        port=5432
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

        CREATE TABLE IF NOT EXISTS userfeature (
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
            user_id_raw INT,
            user_id INT,
            created TIMESTAMP,
            datetime TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS itemfeature (
            item_category INT,
            item_shop INT,
            item_brand INT,
            item_id_raw INT,
            item_id INT,
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
