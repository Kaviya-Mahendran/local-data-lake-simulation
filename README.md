Local Data Lake Simulation

This project sets up and demonstrates a foundational data lake pipeline running entirely on your local machine. It leverages LocalStack Community Edition to emulate essential AWS cloud services (specifically S3) and uses Python for all data processing and analysis tasks.

It's designed to give you hands-on experience with the key stages of a data lake: Data Ingestion, Data Transformation (ETL), and Data Analysis/Querying, moving data from raw inputs to processed, query-ready formats.

Features
LocalStack S3: Acts as the scalable data storage layer, emulating Amazon S3 for both raw and processed data "zones."
Python with Pandas & Boto3: The core engine for:
Data Ingestion: Uploading raw CSV files into S3.
Data Transformation (ETL): Reading raw data, performing in-memory transformations (like calculating total_price), and writing the refined data back to S3 in an optimized format (Parquet).
Data Analysis: Directly querying and analyzing the processed Parquet data from S3 using Pandas to derive immediate insights.


Technologies Used
Docker: Essential for running LocalStack containers.
LocalStack Community Edition: Your local AWS cloud emulator, providing S3 functionality.
Python 3 (3.8+): The primary scripting language for orchestrating the pipeline.
boto3: The official AWS SDK for Python, used for programmatic interaction with LocalStack S3.
Pandas: A powerful and widely used Python library for data manipulation and analysis.

Getting Started
Follow these steps to quickly set up and run the entire data lake simulation on your local machine.

Prerequisites
Make sure you have these installed before you begin:

Docker Desktop: Running and available.
Python 3.8+: Ensure python3 and pip are accessible from your terminal.
1. Clone the Repository
Start by cloning this project's repository to your local machine:



git clone <YOUR_REPOSITORY_URL> # Replace with your actual GitHub repository URL
cd data-lake-simulation-localstack # Navigate into your project directory
2. Install Python Dependencies to manage project dependencies.


python3 -m venv venv


pip install -r requirements.txt


3. Start LocalStack
This will download the LocalStack Docker image (if not already present) and start the LocalStack container in the background.



docker compose up -d


Initialization Time: LocalStack services can take 30-60 seconds (or sometimes a bit longer) to fully initialize after the container starts.
Verify Readiness: To confirm LocalStack is ready, check its logs:


docker compose logs localstack
Look for the line LocalStack is ready. before proceeding.
Troubleshooting Device or resource busy / Connection refused: If LocalStack repeatedly fails to start with "Device or resource busy" or if Python scripts get "Connection refused":
Stop LocalStack: docker compose down
Quit Docker Desktop completely from your system tray/menu bar.
Restart Docker Desktop and wait for it to fully load.
Manually remove the LocalStack data directory: rm -rf localstack_data (ensure you are in the project root directory).
Try docker compose up -d again, wait, and check logs.
Running the Data Lake Pipeline
Once LocalStack is up and running, you can execute the pipeline scripts sequentially.

1. Data Ingestion
This script will read data from your local data/raw folder and upload it into the designated S3 raw data bucket within LocalStack.


python3 scripts/ingest_data.py
2. Data Transformation (ETL)
This script will read the raw data from S3, perform necessary transformations (e.g., add total_price column), and then save the refined data into the processed S3 bucket in Parquet format.



python3 scripts/transform_data.py # Adjust script name if different (e.g., setup_glue.py)
3. Data Analysis/Querying
This script demonstrates how to directly query and analyze the processed Parquet data stored in LocalStack S3 using Pandas.


python3 scripts/analyze_data.py # Adjust script name if different (e.g., query_data.py)
Cleanup
To stop LocalStack and free up system resources, run:



docker compose down
This command will stop and remove the LocalStack container and network created by docker compose up -d.

To remove all Docker resources not currently in use (containers, images, volumes, networks), which can be useful for freeing up disk space:


docker system prune -a --volumes
📂 Project Structure
.
├── data/                    # Contains sample raw data and where processed data will be saved
│   └── raw/                 # Example: sample_sales_data.csv
├── scripts/                 # Python scripts for orchestrating the data lake pipeline
│   ├── ingest_data.py       # Handles uploading raw data to S3
│   ├── transform_data.py    # Reads raw data, transforms, and saves processed data (e.g., Parquet)
│   └── analyze_data.py      # Queries and analyzes the processed data
├── docker-compose.yml       # Defines the LocalStack service configuration
├── requirements.txt         # Lists Python package dependencies (boto3, pandas)
└── README.md                # This project's documentation file
