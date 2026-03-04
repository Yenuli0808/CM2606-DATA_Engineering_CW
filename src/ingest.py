import pandas as pd
import os

def ingest_data(file_path):
    """
    Reads raw CSV file from Dataset folder.
    Returns pandas DataFrame.
    """
    try:
        df = pd.read_csv(file_path)
        print("✅ Data successfully ingested.")
        print(f"Shape: {df.shape}")
        return df
    except Exception as e:
        print("❌ Error during ingestion:", e)
        raise