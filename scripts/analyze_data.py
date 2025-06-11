import boto3
import pandas as pd
import io

# Configuring boto3 to use LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",  # LocalStack default
    aws_secret_access_key="test",  # LocalStack default
    region_name="us-east-1",  # LocalStack default region
)

PROCESSED_BUCKET_NAME = "raw-data-lake-bucket"
PROCESSED_FILE_KEY = "processed_sales/sales_with_total_price.parquet"

print(f"Reading processed data from s3://{PROCESSED_BUCKET_NAME}/{PROCESSED_FILE_KEY}...")

try:
    # Getting the Parquet file from S3
    obj = s3.get_object(Bucket=PROCESSED_BUCKET_NAME, Key=PROCESSED_FILE_KEY)

    # Read the Parquet content into a pandas DataFrame
    df_processed = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    print("Processed data read successfully. Performing analysis...\n")

    # Analysis Operations ---

    print("--- First 5 Rows of Processed Data ---")
    print(df_processed.head())
    print("\n------------------------------------\n")

    print("--- Summary Statistics ---")
    print(df_processed.describe())
    print("\n------------------------------------\n")

    # Corrected Analysis: Total Sales by Product Name (as category) ---
    print("--- Total Sales by Product Name (used as Category) ---")
    sales_by_product_name = df_processed.groupby('product_name')['total_price'].sum().reset_index()
    print(sales_by_product_name)
    print("\n------------------------------------\n")

    print("Analysis complete.")

except Exception as e:
    print(f"An error occurred during analysis: {e}")