import boto3 
import pandas as pd
from io import StringIO
import os

def ingest_data(file_path=None):
    print("📥 Reading dataset from S3...")

    bucket = "superstore-data-lake-de"
    key = "raw/superstore.csv"

    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=bucket, Key=key)

    df = pd.read_csv(obj["Body"])

    print("✅ Data successfully ingested from S3.")
    print("Shape:", df.shape)

    return df
