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
    """Sequence: Download -> Upload -> Delete Local"""
    t, y, m = args
    
    # Define file name
    csv_file = f"{t}_tripdata_{y}-{m}.csv.gz"
    
    # Define paths
    url = f"{BASE_URL}{t}/{csv_file}"
    local_csv_path = os.path.join(DOWNLOAD_DIR, csv_file)

    try:
        # 1. Download
        print(f"Downloading {csv_file}...")
        urllib.request.urlretrieve(url, local_csv_path)

        # 2. Upload .csv.gz to GCS
        print(f"Uploading {csv_file} to GCS...")
        blob = bucket.blob(f"{t}/{csv_file}") 
        blob.upload_from_filename(local_csv_path)

        # 3. Cleanup
        os.remove(local_csv_path)
        print(f"Successfully processed and cleaned up: {csv_file}")

    except Exception as e:
        print(f"Error processing {csv_file}: {e}")
        # Cleanup if file exists despite error
        if os.path.exists(local_csv_path):
            os.remove(local_csv_path)

if __name__ == "__main__":
    # Ensure local directory exists
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)

    create_bucket_if_not_exists(BUCKET_NAME)

    # Prepare combinations
    tasks = [(t, y, m) for t in TYPES for y in YEARS for m in MONTHS]

    # Concurrent processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_file, tasks)

    print("--- All processes complete ---")