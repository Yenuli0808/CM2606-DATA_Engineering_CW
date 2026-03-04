import os
from ingest import ingest_data
from transform import transform_data
from load import load_data

def run_pipeline():

    raw_file_path = os.path.join("..","Dataset", "superstore.csv")
    processed_file_path = os.path.join("..","Dataset", "processed_superstore.csv")

    # Step 1: Ingest
    df = ingest_data(raw_file_path)

    # Step 2: Transform
    df_transformed = transform_data(df)

    # Step 3: Load
    load_data(df_transformed, processed_file_path)

    print("🚀 Pipeline executed successfully!")

if __name__ == "__main__":
    run_pipeline()