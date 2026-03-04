import os

def load_data(df, output_path):
    """
    Saves transformed data into processed folder.
    """

    try:
        df.to_csv(output_path, index=False)
        print("✅ Data successfully saved to processed folder.")
    except Exception as e:
        print("❌ Error during loading:", e)
        raise