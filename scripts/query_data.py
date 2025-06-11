import pandas as pd
import boto3
import io

# --- LocalStack Configuration 
LOCALSTACK_ENDPOINT = 'http://localhost:4566'
AWS_ACCESS_KEY_ID = 'test'
AWS_SECRET_ACCESS_KEY = 'test'
AWS_REGION = 'us-east-1'

s3_client = boto3.client(
    's3',
    endpoint_url=LOCALSTACK_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# --- Data Lake Paths ---
PROCESSED_BUCKET_NAME = 'processed-data-lake-bucket' # Source bucket for transformed data
PROCESSED_S3_KEY = 'sales/transformed_sales_data.csv' # Key for the transformed data file

def query_simulated_data():
    """
    Simulates querying processed data to extract insights.
    Downloads processed data and performs Pandas operations similar to SQL queries.
    """
    print("Starting data querying simulation...")
    try:
        # 1. Download processed data from S3
        obj = s3_client.get_object(Bucket=PROCESSED_BUCKET_NAME, Key=PROCESSED_S3_KEY)
        df_processed = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print(f"Processed data loaded from s3://{PROCESSED_BUCKET_NAME}/{PROCESSED_S3_KEY} for querying.")

        # 2. Simulate a SQL-like query: Total sales by product
        # This is like `SELECT product_name, SUM(total_price) FROM df_processed GROUP BY product_name ORDER BY total_price DESC`
        sales_by_product = df_processed.groupby('product_name')['total_price'].sum().reset_index()
        sales_by_product = sales_by_product.sort_values(by='total_price', ascending=False)

        print("\n--- Simulated Query Result: Total Sales by Product ---")
        print(sales_by_product.to_string(index=False)) # .to_string() makes it print nicely in console

        # 3. Simulate another query: Daily total sales
        # Convert 'order_date' to datetime objects for date-based grouping
        df_processed['order_date'] = pd.to_datetime(df_processed['order_date'])
        daily_sales = df_processed.groupby('order_date')['total_price'].sum().reset_index()
        daily_sales = daily_sales.sort_values(by='order_date')

        print("\n--- Simulated Query Result: Daily Total Sales ---")
        print(daily_sales.to_string(index=False))

        print("\nData querying simulation complete.")

    except s3_client.exceptions.NoSuchBucket as e:
        print(f"Error: Bucket '{PROCESSED_BUCKET_NAME}' not found. Ensure transformation step ran successfully.")
        raise e # Re-raise to fail pipeline
    except s3_client.exceptions.NoSuchKey as e:
        print(f"Error: Key '{PROCESSED_S3_KEY}' not found in bucket '{PROCESSED_BUCKET_NAME}'. Ensure transformation step ran successfully.")
        raise e # Re-raise to fail pipeline
    except Exception as e:
        print(f"Error during data querying simulation: {e}")
        raise # Re-raise to fail pipeline

if __name__ == "__main__":
    query_simulated_data()