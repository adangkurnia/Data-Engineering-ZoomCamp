import os
import sys
import urllib.request
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

# --- Configuration ---
BUCKET_NAME = "dezoomcamp_04_analytics_engineering"
CREDENTIALS_FILE = "gcs.json"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
TYPES = ['green', 'yellow']
YEARS = [2019, 2020]
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "temp_data"

# Initialize GCS Client
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

def create_bucket_if_not_exists(bucket_name):
    try:
        client.get_bucket(bucket_name)
        print(f"Verified bucket: {bucket_name}")
    except NotFound:
        client.create_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    except Forbidden:
        print(f"Access denied for bucket: {bucket_name}")
        sys.exit(1)

def process_file(args):
    """Sequence: Download -> Convert to Parquet -> Upload -> Delete Local"""
    t, y, m = args
    
    # Define file names
    csv_file = f"{t}_tripdata_{y}-{m}.csv.gz"
    parquet_file = csv_file.replace(".csv.gz", ".parquet")
    
    # Define paths
    url = f"{BASE_URL}{t}/{csv_file}"
    local_csv_path = os.path.join(DOWNLOAD_DIR, csv_file)
    local_parquet_path = os.path.join(DOWNLOAD_DIR, parquet_file)

    try:
        # 1. Download
        print(f"Downloading {csv_file}...")
        urllib.request.urlretrieve(url, local_csv_path)

        # 2. Convert CSV to Parquet
        print(f"Converting {csv_file} to Parquet...")
        df = pd.read_csv(local_csv_path, compression='gzip', low_memory=False)
        
        # Optional: Cast data types here if you encounter schema issues in BigQuery
        df.to_parquet(local_parquet_path, engine='pyarrow')

        # 3. Upload Parquet to GCS
        print(f"Uploading {parquet_file} to GCS...")
        blob = bucket.blob(f"{t}/{parquet_file}") # Uploading into type-specific folders in GCS
        blob.upload_from_filename(local_parquet_path)

        # 4. Cleanup
        os.remove(local_csv_path)
        os.remove(local_parquet_path)
        print(f"Successfully processed and cleaned up: {parquet_file}")

    except Exception as e:
        print(f"Error processing {csv_file}: {e}")
        # Cleanup if files exist despite error
        for f in [local_csv_path, local_parquet_path]:
            if os.path.exists(f):
                os.remove(f)

if __name__ == "__main__":
    # Ensure local directory exists
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    create_bucket_if_not_exists(BUCKET_NAME)

    # Prepare combinations
    tasks = [(t, y, m) for t in TYPES for y in YEARS for m in MONTHS]

    # Use ThreadPoolExecutor for concurrent processing
    # Note: max_workers=4 is safe; if you have high RAM, you can increase this.
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_file, tasks)

    print("--- All processes complete ---")