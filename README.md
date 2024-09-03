# Zillow Data Processor

## Overview

The Zillow Data Processor is a sophisticated ETL (Extract, Transform, Load) pipeline designed to process large volumes of scraped Zillow data. It leverages AWS services to create a scalable, efficient, and automated data processing solution.

## Features

- Processes 1GB of daily scraped CSV files from AWS S3
- Implements data cleaning and standardization for 180 columns
- Utilizes medallion architecture for data quality management
- Stores processed data in Parquet format with Hive partitioning
- Deployed as an AWS Lambda function with scheduled execution via CloudWatch
- Optimized for 3GB RAM allocation

## Architecture

The project follows a medallion architecture:

1. **Bronze Layer**: Raw data ingested from S3
2. **Silver Layer**: Cleaned and standardized data
3. **Gold Layer**: Fully processed and enriched data

## Technologies Used

- Python 3.8+
- AWS Services:
  - S3 for data storage
  - Lambda for serverless computation
  - CloudWatch for scheduling
- pandas for data manipulation
- pyarrow for Parquet file handling
- boto3 for AWS SDK

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/rohanredtj/aws-zillow-pipeline.git
   ```

2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

3. Set up AWS credentials in your environment variables or AWS credentials file.

## Usage

The main processing script is designed to run as an AWS Lambda function. However, you can test it locally:

```python
from zillow_processor import process_s3_files

source_bucket = 'your-source-bucket'
source_prefix = 'raw-data/'
destination_bucket = 'your-destination-bucket'
destination_prefix = 'processed-data/'

process_s3_files(source_bucket, source_prefix, destination_bucket, destination_prefix)
```

## Data Processing

The processor performs the following key operations:

1. **Data Extraction**: Reads CSV files from the source S3 bucket.
2. **Data Cleaning**: 
   - Converts data types (e.g., strings to integers/floats)
   - Handles missing values
   - Standardizes date formats
3. **Data Transformation**:
   - Normalizes JSON columns
   - Converts string representations of arrays and booleans
4. **Data Loading**: Writes processed data to the destination S3 bucket in Parquet format with Hive partitioning.

## AWS Lambda Deployment

The processor is deployed as an AWS Lambda function:

- **Runtime**: Python 3.8
- **Memory**: 3GB RAM allocation
- **Timeout**: Configured based on average processing time
- **Trigger**: CloudWatch Events for daily scheduled execution

## Monitoring and Logging

- CloudWatch Logs for Lambda function logs
- Custom metrics pushed to CloudWatch for monitoring data volume and processing time

## Contact

For any questions or suggestions, please open an issue or contact at rohan.rathore93@example.com
