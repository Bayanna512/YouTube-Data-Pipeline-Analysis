import json
import requests
import pandas as pd
import boto3
from io import StringIO
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize Spark Context and Session
sc = SparkContext()
spark = SparkSession(sc)

# S3 Client
s3_client = boto3.client('s3')

# Function to read data from S3
def read_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    return data

# Read API keys from S3
def read_api_keys_from_s3(bucket, key):
    data = read_from_s3(bucket, key)
    api_keys = json.loads(data)
    return api_keys['VIDEO_API_KEYS']

# Read channel IDs from S3
def read_channel_ids_from_s3(bucket, key):
    data = read_from_s3(bucket, key)
    df = pd.read_csv(StringIO(data))
    return df

# Fetch channel details
def fetch_channel_details(api_key, channel_id):
    url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&id={channel_id}&key={api_key}'
    response = requests.get(url)
    data = response.json()
    if 'items' in data and data['items']:
        item = data['items'][0]
        return {
            'Channel ID': item['id'],
            'Channel Name': item['snippet']['title'],
            'Subscriber Count': item['statistics'].get('subscriberCount', 0),
            'Video Count': item['statistics'].get('videoCount', 0)
        }
    return None

# S3 details
S3_BUCKET = 'sphtest512'
API_KEYS_S3_KEY = 'data/API_keys.json'
CHANNEL_IDS_S3_KEY = 'data/channel_ids.csv'
RESULTS_S3_KEY = 'data/channel_details/channel_details.csv'

# Fetch API keys and channel IDs
api_keys = read_api_keys_from_s3(S3_BUCKET, API_KEYS_S3_KEY)
channel_ids_df = read_channel_ids_from_s3(S3_BUCKET, CHANNEL_IDS_S3_KEY)

# Fetch channel details using the first API key (for simplicity)
api_key = api_keys[0]
results = []
for _, row in channel_ids_df.iterrows():
    channel_id = row['Channel ID']
    details = fetch_channel_details(api_key, channel_id)
    if details:
        results.append(details)

# Convert results to DataFrame and CSV
results_df = pd.DataFrame(results)
csv_buffer = StringIO()
results_df.to_csv(csv_buffer, index=False)

# Upload results to S3
s3_client.put_object(Bucket=S3_BUCKET, Key=RESULTS_S3_KEY, Body=csv_buffer.getvalue())

print(f'Successfully uploaded channel details CSV to S3 bucket "{S3_BUCKET}" at "{RESULTS_S3_KEY}"')
