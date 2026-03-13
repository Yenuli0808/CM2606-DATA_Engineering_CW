import os
import time
from ingest import ingest_data
from transform import transform_data, validate_data
from load import load_data
from logger import setup_logger

def run_pipeline():
    logger = setup_logger()
    logger.info("Pipeline started")
    start = time.time()

    raw_file_path = os.path.join( "data_lake", "raw", "superstore.csv")
    processed_file_path = os.path.join( "data_lake", "processed", "processed_superstore.csv")

    # Step 1: Ingest
    df = ingest_data(raw_file_path)
    logger.info("Data ingestion completed")

    # Step 2: Transform
    df_transformed = transform_data(df)
    logger.info("Transformation completed")

    # Step 3: Validate
    validate_data(df_transformed)
    logger.info("Data validation completed")

    # Step 4: Load
    load_data(df_transformed, processed_file_path)
    logger.info("Loading completed")

    print("🚀 Pipeline executed successfully!")
    end = time.time()
    print(f"⏱ Pipeline finished in {end-start:.2f} seconds")

if __name__ == "__main__":
    run_pipeline()
