import boto3
import os

# --- LocalStack Configuration ---
LOCALSTACK_ENDPOINT = 'http://localhost:4566' # The address where LocalStack is running
AWS_ACCESS_KEY_ID = 'test' # Dummy credentials for LocalStack
AWS_SECRET_ACCESS_KEY = 'test' # Dummy credentials for LocalStack
AWS_REGION = 'us-east-1' # Dummy region

# Configure boto3 client to connect to LocalStack's S3 service
s3_client = boto3.client(
    's3',
    endpoint_url=LOCALSTACK_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# --- Data Lake Paths 
RAW_BUCKET_NAME = 'raw-data-lake-bucket' # Name of the S3 bucket for raw data
RAW_S3_KEY = 'sales/raw_sales_data.csv' # Path within the bucket where the file will be stored
LOCAL_FILE_PATH = 'data/sample_sales_data.csv' # Path to your local sample data file

def create_bucket_if_not_exists(bucket_name):
    """
    Checks if an S3 bucket exists and creates it if it doesn't.
    This is important because you can't upload to a non-existent bucket.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name) # Try to get information about the bucket
        print(f"Bucket '{bucket_name}' already exists.")
    except s3_client.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404: # 404 means 'Not Found' - the bucket doesn't exist
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            raise e # Re-raise other errors (e.g., permissions)

def ingest_raw_data():
    """
    Main function to ingest raw data.
    It creates the bucket (if needed) and uploads the local CSV file to S3.
    """
    print(f"Starting data ingestion for {LOCAL_FILE_PATH}...")
    try:
        # Ensure the raw data bucket exists
        create_bucket_if_not_exists(RAW_BUCKET_NAME)

        # Upload the local file to the specified S3 bucket and key
        s3_client.upload_file(LOCAL_FILE_PATH, RAW_BUCKET_NAME, RAW_S3_KEY)

        print(f"Successfully ingested data to s3://{RAW_BUCKET_NAME}/{RAW_S3_KEY}")
    except Exception as e:
        print(f"Error during data ingestion: {e}")
        # Re-raise the exception to indicate failure if run in a pipeline
        raise 

if __name__ == "__main__":
    # This block runs when the script is executed directly
    ingest_raw_data()