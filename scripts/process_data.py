import boto3
import pandas as pd
import io

# Configure boto3 to use LocalStack
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="test",  # LocalStack default
    aws_secret_access_key="test",  # LocalStack default
    region_name="us-east-1",  # LocalStack default region
)

RAW_BUCKET_NAME = "raw-data-lake-bucket"
RAW_FILE_KEY = "sales/raw_sales_data.csv"
PROCESSED_PREFIX = "processed_sales/"
PROCESSED_FILE_NAME = "sales_with_total_price.parquet"

print(f"Reading raw data from s3://{RAW_BUCKET_NAME}/{RAW_FILE_KEY}...")

try:
    # the CSV file from S3
    obj = s3.get_object(Bucket=RAW_BUCKET_NAME, Key=RAW_FILE_KEY)

    # Read the CSV content into a pandas DataFrame
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    print("Raw data read successfully. Performing transformation...")

    # --- Data Transformation: 
    # Use the correct column names: 'unit_price' and 'quantity'
    # These columns are already numeric (float64 and int64 as per your df.info() output)
    df["total_price"] = df["unit_price"] * df["quantity"]

    print("Transformation complete. Writing processed data to S3...")

    # Save the transformed DataFrame to a Parquet file in-memory
    # Parquet is a columnar format, much better for analytics than CSV
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0) # Rewind the buffer to the beginning

    # Upload the Parquet file to S3
    s3.put_object(
        Bucket=RAW_BUCKET_NAME,
        Key=f"{PROCESSED_PREFIX}{PROCESSED_FILE_NAME}",
        Body=parquet_buffer,
        ContentType="application/x-parquet" # Set correct content type
    )

    print(f"Successfully processed data and saved to s3://{RAW_BUCKET_NAME}/{PROCESSED_PREFIX}{PROCESSED_FILE_NAME}")

except Exception as e:
    print(f"An error occurred: {e}")