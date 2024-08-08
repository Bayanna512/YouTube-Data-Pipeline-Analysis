
import requests
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import boto3
from io import StringIO
import pandas as pd

def fetch_channel_id(api_key, handle):
    url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&q={handle}&key={api_key}'
    response = requests.get(url)
    data = response.json()
    if 'items' in data and data['items']:
        return data['items'][0]['id']['channelId']
    return None

# Initialize Spark Context and Session
sc = SparkContext()
spark = SparkSession(sc)

# List of custom handles
CUSTOM_HANDLES = [
    'straitstimesonline',
    'TheBusinessTimes',
    'zaobaosg',
    'Tamil_Murasu',
    'BeritaHarianSG1957'
]

API_KEY = 'AIzaSyD9wyRmGCOihj5_r46cD3mrhX2UmrjrnFk'
S3_BUCKET = 'sphtest512'
S3_KEY = 'data/channel_details/channel_ids.csv'

# Create a Spark DataFrame
def fetch_data():
    data = [(handle, fetch_channel_id(API_KEY, handle)) for handle in CUSTOM_HANDLES]
    df = spark.createDataFrame(data, ["Handle", "Channel ID"])
    return df

df = fetch_data()

# Convert DataFrame to Pandas DataFrame for CSV conversion
pandas_df = df.toPandas()
csv_buffer = StringIO()
pandas_df.to_csv(csv_buffer, index=False)

# Upload CSV to S3
s3_client = boto3.client('s3')
s3_client.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=csv_buffer.getvalue())

print(f'Successfully uploaded CSV to S3 bucket "{S3_BUCKET}" at "{S3_KEY}"')
