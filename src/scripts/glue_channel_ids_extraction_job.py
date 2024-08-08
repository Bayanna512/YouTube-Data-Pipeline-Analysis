import requests
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import boto3
from io import StringIO
import pandas as pd

# Function to fetch the YouTube channel ID using a custom handle and API key
def fetch_channel_id(api_key, handle):
    """
    Fetches the YouTube channel ID for a given custom handle using the YouTube Data API.

    Parameters:
    api_key (str): The API key to access the YouTube Data API.
    handle (str): The custom handle of the YouTube channel.

    Returns:
    str: The channel ID if found, otherwise None.
    """
    url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={handle}&key={api_key}'
    response = requests.get(url)
    data = response.json()
    if 'items' in data and data['items']:
        return data['items'][0]['id']['channelId']
    return None

# Initialize Spark Context and Session for processing data with PySpark
sc = SparkContext()
spark = SparkSession(sc)

# List of custom YouTube channel handles to be processed
CUSTOM_HANDLES = [
    'straitstimesonline',
    'TheBusinessTimes',
    'zaobaosg',
    'Tamil_Murasu',
    'BeritaHarianSG1957'
]

# API key and S3 details
API_KEY = 'AIzaSyD9wyRmGCOihj5_r46cD3mrhX2UmrjrnFk'
S3_BUCKET = 'sphtest512'
S3_KEY = 'data/channel_details/channel_ids.csv'

# Function to fetch data and create a Spark DataFrame
def fetch_data():
    """
    Fetches YouTube channel IDs for the provided custom handles and stores the data in a Spark DataFrame.

    Returns:
    DataFrame: A Spark DataFrame containing the custom handles and their corresponding channel IDs.
    """
    data = [(handle, fetch_channel_id(API_KEY, handle)) for handle in CUSTOM_HANDLES]
    df = spark.createDataFrame(data, ["Handle", "Channel ID"])
    return df

# Fetch data and create a DataFrame
df = fetch_data()

# Convert Spark DataFrame to Pandas DataFrame for easier CSV conversion
pandas_df = df.toPandas()
csv_buffer = StringIO()
pandas_df.to_csv(csv_buffer, index=False)

# Upload the resulting CSV file to the specified S3 bucket and key
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=csv_buffer.getvalue())

print(f'Successfully uploaded CSV to S3 bucket \"{S3_BUCKET}\" at \"{S3_KEY}\"')
