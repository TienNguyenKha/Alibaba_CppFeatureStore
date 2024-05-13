from minio import Minio
from helpers import load_cfg
from glob import glob
import os

def push_minIO(client, datalake_cfg, data_cfg , is_train= True):
    data_type = "train" if is_train else "test"
    # Upload files.
    all_fps = glob(
        os.path.join(
            data_cfg[f"folder_path_{data_type}"],
            "*.csv"
        )
    )

    for fp in all_fps:
        print(f"Uploading {fp}")
        client.fput_object(
            bucket_name=datalake_cfg["bronze_bucket_name"],
            object_name=os.path.join(
                datalake_cfg[f"bronze_folder_name_{data_type}"],
                os.path.basename(fp)
            ),
            file_path=fp
        )

def ingest_data(CFG_FILE ,upload_train=True, upload_test=True):
    cfg = load_cfg(CFG_FILE)
    datalake_cfg = cfg["minIO_config"]
    data_cfg = cfg["alicpp_raw_data"]

    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        endpoint=datalake_cfg["endpoint"],
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,
    )
    
    # Create bucket if not exist.
    found = client.bucket_exists(bucket_name=datalake_cfg["bronze_bucket_name"])
    if not found:
        client.make_bucket(bucket_name=datalake_cfg["bronze_bucket_name"])
    else:
        print(f'Bucket {datalake_cfg["bronze_bucket_name"]} already exists, skip creating!')
    
    if upload_train:
        push_minIO(client, datalake_cfg, data_cfg, is_train= True)
    
    if upload_test:
        push_minIO(client, datalake_cfg, data_cfg, is_train= False)

if __name__ == '__main__':
    CFG_FILE = "../../config.yaml"
    ingest_data(CFG_FILE, upload_train=True, upload_test=True)
    