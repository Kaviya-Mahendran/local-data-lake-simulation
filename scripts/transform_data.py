import pandas as pd
import boto3
import io # Used for reading/writing data from/to S3 in memory

# --- LocalStack Configuration (Same as ingest_data.py) ---
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
RAW_BUCKET_NAME = 'raw-data-lake-bucket' # Source bucket for raw data
RAW_S3_KEY = 'sales/raw_sales_data.csv' # Key for the raw data file
PROCESSED_BUCKET_NAME = 'processed-data-lake-bucket' # Destination bucket for transformed data
PROCESSED_S3_KEY = 'sales/transformed_sales_data.csv' # Key for the transformed data file

def create_bucket_if_not_exists(bucket_name):
    """
    (Helper function, same as in ingest_data.py)
    Checks if an S3 bucket exists and creates it if it doesn't.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except s3_client.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            raise e

def transform_data():
    """
    Main function for data transformation.
    Downloads raw data, calculates 'total_price', and uploads to a new S3 location.
    """
    print("Starting data transformation...")
    try:
        # 1. Download raw data from S3
        # get_object returns a dict, 'Body' contains the file content as a stream
        obj = s3_client.get_object(Bucket=RAW_BUCKET_NAME, Key=RAW_S3_KEY)
        # Use io.BytesIO to treat the byte stream as a file for pandas
        df_raw = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print(f"Raw data loaded from s3://{RAW_BUCKET_NAME}/{RAW_S3_KEY}.")
        print("First 3 rows of raw data:")
        print(df_raw.head(3).to_string(index=False)) # print a few rows for verification

        # 2. Perform transformations: Calculate a new column 'total_price'
        df_transformed = df_raw.copy() # Good practice to work on a copy
        df_transformed['total_price'] = df_transformed['quantity'] * df_transformed['unit_price']
        print("Data transformed: 'total_price' column calculated.")
        print("First 3 rows of transformed data:")
        print(df_transformed.head(3).to_string(index=False))

        # 3. Upload transformed data to a new S3 bucket/key
        create_bucket_if_not_exists(PROCESSED_BUCKET_NAME)

        # Convert DataFrame to CSV string in memory before uploading
        csv_buffer = io.StringIO()
        df_transformed.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=PROCESSED_BUCKET_NAME,
            Key=PROCESSED_S3_KEY,
            Body=csv_buffer.getvalue() # Get the string value from the buffer
        )
        print(f"Successfully uploaded transformed data to s3://{PROCESSED_BUCKET_NAME}/{PROCESSED_S3_KEY}")
    except Exception as e:
        print(f"Error during data transformation: {e}")
        raise # Re-raise to fail pipeline if needed

if __name__ == "__main__":
    transform_data()