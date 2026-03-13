import pandas as pd

def transform_data(df):
    """
    Performs data cleaning and transformation.
    """

    print("🔄 Starting data transformation...")

    # Drop unnecessary columns
    columns_to_drop = ["记录数", "Row.ID", "Market2"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # Convert date columns
    df["Order.Date"] = pd.to_datetime(df["Order.Date"])
    df["Ship.Date"] = pd.to_datetime(df["Ship.Date"])

    # Remove duplicates (even if 0 now – good practice)
    df = df.drop_duplicates()

    # Ensure numeric columns are correct type
    df["Sales"] = pd.to_numeric(df["Sales"])
    df["Profit"] = pd.to_numeric(df["Profit"])
    df["Quantity"] = pd.to_numeric(df["Quantity"])
    df["Discount"] = pd.to_numeric(df["Discount"])
    df["Shipping.Cost"] = pd.to_numeric(df["Shipping.Cost"])

    # Create YearMonth column for aggregation
    df["YearMonth"] = df["Order.Date"].dt.to_period("M")

    print("✅ Transformation completed.")
    return df

def validate_data(df):

    # Check dataset is not empty
    if df.empty:
        raise ValueError("Dataset is empty!")

    # Check required columns exist
    required_columns = [
        "Customer.ID",
        "Product.ID",
        "Sales",
        "Quantity",
        "Order.Date"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Check for missing values
    if df.isnull().sum().sum() > 0:
        raise ValueError("Data contains missing values")

    # Check duplicate rows
    if df.duplicated().sum() > 0:
        print("⚠ Warning: duplicate rows detected")

    # Check negative sales values
    if (df["Sales"] < 0).any():
        raise ValueError("Negative sales values detected")

    # Check invalid quantity
    if (df["Quantity"] <= 0).any():
        raise ValueError("Invalid quantity values detected")

    # Validate order date format
    df["Order.Date"] = pd.to_datetime(df["Order.Date"], errors="coerce")

    if df["Order.Date"].isnull().any():
        raise ValueError("Invalid date format detected")

    print("✅ Data validation passed")

