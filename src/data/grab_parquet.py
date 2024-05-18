from minio import Minio
import urllib.request
import pandas as pd
from minio import Minio
import sys

def main():
    grab_data()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    base_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_"
    months = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08"]
    save_path = "../../data/raw/"

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    for month in months:
        file_url = f"{base_url}{month}.parquet"
        file_path = os.path.join(save_path, f"yellow_tripdata_{month}.parquet")
        print(f"Downloading {file_url} to {file_path}...")
        urllib.request.urlretrieve(file_url, file_path)
        print(f"Downloaded {file_url} to {file_path}")

def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "nyc-taxi-data"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    folder_path = "../../data/raw/"
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".parquet"):
                file_path = os.path.join(folder_path, file_name)
                print(f"Téléchargement de {file_name} vers le bucket Minio {bucket}...")
                client.fput_object(bucket, file_name, file_path)
                print(f"Téléchargé {file_name}  vers le bucket Minio {bucket}")

if __name__ == '__main__':
    sys.exit(main())
