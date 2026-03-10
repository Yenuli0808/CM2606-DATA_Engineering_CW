import pandas as pd
from sqlalchemy import create_engine, text

def load_data(df, output_path):

    print("🚀 Starting load stage...")

    # -----------------------------
    # 1.Save processed CSV
    # -----------------------------
    try:
        df.to_csv(output_path, index=False)
        print("✅ Processed CSV saved.")
    except Exception as e:
        print("❌ Error saving CSV:", e)
        raise


    # -----------------------------
    # 2.Load into PostgreSQL
    # -----------------------------
    print("🚀 Loading data into PostgreSQL warehouse...")

    engine = create_engine(
        "postgresql://polaar@localhost:5432/superstore_dw"
    )
    # Clear tables before loading
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE fact_sales, dim_customer, dim_product, dim_date CASCADE"))
        conn.commit()

    print("🧹 Warehouse tables cleared.")

    # ---------- DIM CUSTOMER ----------
    dim_customer = df[[
        "Customer.ID",
        "Customer.Name",
        "Segment",
        "Country",
        "Region",
        "City",
        "State"
    ]].drop_duplicates(subset=["Customer.ID"]) 

    dim_customer.columns = [
        "customer_id",
        "customer_name",
        "segment",
        "country",
        "region",
        "city",
        "state"
    ]

    dim_customer.to_sql(
        "dim_customer",
        engine,
        if_exists="append",
        index=False
    )


    # ---------- DIM PRODUCT ----------
    dim_product = df[[
        "Product.ID",
        "Product.Name",
        "Category",
        "Sub.Category"
    ]].drop_duplicates(subset=["Product.ID"])

    dim_product.columns = [
        "product_id",
        "product_name",
        "category",
        "sub_category"
    ]

    dim_product.to_sql(
        "dim_product",
        engine,
        if_exists="append",
        index=False
    )


    # ---------- DIM DATE ----------
    dim_date = df[[
        "Order.Date",
        "Year",
        "weeknum"
    ]].drop_duplicates()

    dim_date["month"] = dim_date["Order.Date"].dt.month

    dim_date.columns = [
        "date_id",
        "year",
        "week_number",
        "month"
    ]

    dim_date = dim_date[[
        "date_id",
        "year",
        "month",
        "week_number"
    ]]

    dim_date.to_sql(
        "dim_date",
        engine,
        if_exists="append",
        index=False
    )


    # ---------- FACT TABLE ----------
    fact_sales = df[[
        "Order.ID",
        "Product.ID",
        "Customer.ID",
        "Order.Date",
        "Sales",
        "Quantity",
        "Profit",
        "Discount",
        "Shipping.Cost"
    ]].drop_duplicates(subset=["Order.ID", "Product.ID"])

    fact_sales.columns = [
        "order_id",
        "product_id",
        "customer_id",
        "order_date",
        "sales",
        "quantity",
        "profit",
        "discount",
        "shipping_cost"
    ]

    fact_sales.to_sql(
        "fact_sales",
        engine,
        if_exists="append",
        index=False
    )

    print("✅ Data successfully loaded into PostgreSQL warehouse!")