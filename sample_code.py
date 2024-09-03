import pandas as pd
import numpy as np
from datetime import datetime
import json
import re
import ast
import io
import boto3
import os

def process_zillow_data(df):
    """
    Process Zillow data, cleaning and transforming various columns.
    """
    df['scraping_datetime'] = pd.to_datetime(df['scraping_datetime'], errors='coerce')
    df['year'] = df['scraping_datetime'].dt.year
    df['month'] = df['scraping_datetime'].dt.month
    df['day'] = df['scraping_datetime'].dt.day

    df = df[df['scraping_datetime'].notnull()]
    df = df[df['zpid'].apply(is_numeric)]
    df['zpid'] = df['zpid'].apply(safe_int_convert).astype('Int64')

    df = clean_json_column(df, 'zillow_search_request')
    df['on_market_date'] = pd.to_datetime(df['on_market_date'], errors='coerce')
    
    df['year_built'] = df['year_built'].apply(safe_int_convert).astype('Int64')
    df['property_subtype'] = df['property_subtype'].apply(convert_to_array)
    df['is_new_construction'] = df['is_new_construction'].apply(convert_to_bool)
    
    df['latitude'] = df['latitude'].apply(safe_float_convert)
    df['zipcode'] = df['zipcode'].apply(safe_int_convert).astype('Int64')
    df['price'] = df['price'].apply(safe_float_convert)
    df['price_history'] = df['price_history'].apply(convert_to_array)
    
    df['bedrooms'] = df['bedrooms'].apply(safe_float_convert)
    df = clean_json_column(df, 'at_a_glance_facts')
    
    return df

def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def safe_int_convert(x):
    if pd.isna(x):
        return x
    try:
        return int(float(x))
    except ValueError:
        return np.nan

def safe_float_convert(x):
    if pd.isna(x):
        return x
    try:
        return float(x)
    except ValueError:
        return np.nan

def clean_json_column(df, column_name):
    df_copy = df.copy()
    df_copy[column_name] = df_copy[column_name].apply(safe_json_loads)
    df_normalized = pd.json_normalize(df_copy[column_name].dropna().tolist()).add_prefix(f"{column_name}_")
    df_normalized.index = df_copy[df_copy[column_name].notna()].index
    df_cleaned = pd.concat([df_copy.drop(columns=[column_name]), df_normalized], axis=1)
    return df_cleaned

def safe_json_loads(x):
    if pd.isna(x):
        return None
    try:
        return json.loads(x)
    except json.JSONDecodeError:
        return None

def convert_to_array(x):
    if pd.isna(x):
        return np.nan
    try:
        return np.array(ast.literal_eval(x))
    except (ValueError, SyntaxError):
        return np.nan

def convert_to_bool(x):
    if isinstance(x, bool):
        return x
    elif pd.isna(x):
        return np.nan
    else:
        return np.nan

def get_s3_client():
    """
    Create and return an S3 client using environment variables for credentials.
    """
    return boto3.client('s3',
                        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
                        region_name=os.environ.get('AWS_REGION'))

def read_csv_from_s3(s3_client, bucket, key):
    """
    Read a CSV file from S3 and return a pandas DataFrame.
    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj['Body'].read()))

def write_csv_to_s3(s3_client, df, bucket, key):
    """
    Write a pandas DataFrame to a CSV file in S3.
    """
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

def process_s3_files(source_bucket, source_prefix, destination_bucket, destination_prefix):
    """
    Process all CSV files in a given S3 bucket and prefix, and save results to another bucket.
    """
    s3_client = get_s3_client()
    
    # List all objects in the source bucket with the given prefix
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
    
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            print(f"Processing {obj['Key']}...")
            
            # Read the CSV file from S3
            df = read_csv_from_s3(s3_client, source_bucket, obj['Key'])
            
            # Process the data
            processed_df = process_zillow_data(df)
            
            # Generate the new key for the processed file
            destination_key = f"{destination_prefix}/{obj['Key'].split('/')[-1].replace('.csv', '_processed.csv')}"
            
            # Write the processed DataFrame back to S3
            write_csv_to_s3(s3_client, processed_df, destination_bucket, destination_key)
            
            print(f"Processed file saved as {destination_key}")

# Example usage
if __name__ == "__main__":
    source_bucket = os.environ.get('SOURCE_BUCKET')
    destination_bucket = os.environ.get('DESTINATION_BUCKET')
    source_prefix = 'raw-data/'
    destination_prefix = 'processed-data/'
    
    process_s3_files(source_bucket, source_prefix, destination_bucket, destination_prefix)