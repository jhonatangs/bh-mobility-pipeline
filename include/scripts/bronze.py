import requests
import os
from datetime import datetime
import pandas as pd


def extract_bus_data():
    """
    Fetches real-time bus data from PBH API and saves as Raw Parquet.
    """
    # URL placeholder: using a search query to simulate getting records
    url = "https://ckan.pbh.gov.br/api/3/action/datastore_search?resource_id=d85a3850-2fbe-443d-82b5-5d9c7e39a584&limit=100"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        records = data["result"]["records"]

        ingestion_time = datetime.now()
        for record in records:
            record["_ingestion_timestamp"] = ingestion_time.isoformat()

        df = pd.DataFrame(records)

        today = ingestion_time.strftime("%Y-%m-%d")
        output_path = f"/opt/airflow/data/bronze/bus_position/partition_date={today}"
        os.makedirs(output_path, exist_ok=True)

        file_name = f"bus_data_{ingestion_time.strftime('%H%M%S')}.parquet"
        df.to_parquet(f"{output_path}/{file_name}", index=False)
        print(f"Bronze layer populated: {output_path}/{file_name}")

    except Exception as e:
        print(f"Error extracting data: {e}")
        raise
