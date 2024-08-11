import json
import requests
import pandas as pd
import boto3
from io import StringIO
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Initialize Spark Context and Session
sc = SparkContext()  # Entry point to Spark functionality, manages the connection to the cluster
spark = SparkSession(sc)  # Used to create DataFrames and execute SQL queries in Spark

# S3 Client
s3_client = boto3.client('s3')  # Initialize Boto3 S3 client to interact with AWS S3

# Function to read data from S3
def read_from_s3(bucket, key):
    """
    Reads data from an S3 bucket and key location.
    
    Parameters:
    bucket (str): Name of the S3 bucket.
    key (str): S3 key (file path).
    
    Returns:
    str: Data read from the S3 object as a string.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)  # Retrieve the S3 object using bucket and key
    data = response['Body'].read().decode('utf-8')  # Read the object content and decode it from bytes to string
    return data

# Function to read API keys from S3
def read_api_keys_from_s3(bucket, key):
    """
    Reads API keys from a JSON file stored in S3.
    
    Parameters:
    bucket (str): Name of the S3 bucket.
    key (str): S3 key (file path).
    
    Returns:
    list: List of API keys.
    """
    data = read_from_s3(bucket, key)  # Read JSON data from S3
    api_keys = json.loads(data)  # Parse the JSON data into a Python dictionary
    return api_keys['VIDEO_API_KEYS']  # Return the list of API keys

# Function to read channel IDs from S3
def read_channel_ids_from_s3(bucket, key):
    """
    Reads YouTube channel IDs from a CSV file stored in S3.
    
    Parameters:
    bucket (str): Name of the S3 bucket.
    key (str): S3 key (file path).
    
    Returns:
    DataFrame: Pandas DataFrame containing channel IDs.
    """
    data = read_from_s3(bucket, key)  # Read CSV data from S3 as a string
    df = pd.read_csv(StringIO(data))  # Convert the string data to a Pandas DataFrame
    return df

# Function to fetch channel details using YouTube API
def fetch_channel_details(api_key, channel_id):
    """
    Fetches channel details from the YouTube API.
    
    Parameters:
    api_key (str): API key for accessing the YouTube API.
    channel_id (str): ID of the YouTube channel.
    
    Returns:
    dict: A dictionary containing channel details such as ID, name, subscriber count, and video count.
    """
    url = f'https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&id={channel_id}&key={api_key}'
    response = requests.get(url)  # Make a GET request to the YouTube API
    data = response.json()  # Parse the JSON response from the API
    if 'items' in data and data['items']:  # Check if the response contains channel data
        item = data['items'][0]  # Extract the first item (channel details) from the response
        return {
            'Channel ID': item['id'],  # Extract and return the Channel ID
            'Channel Name': item['snippet']['title'],  # Extract and return the Channel Name
            'Subscriber Count': item['statistics'].get('subscriberCount', 0),  # Extract and return Subscriber Count
            'Video Count': item['statistics'].get('videoCount', 0)  # Extract and return Video Count
        }
    return None  # Return None if no details are found

# S3 bucket and key details
S3_BUCKET = 'sphtest512'  # Define the name of the S3 bucket
API_KEYS_S3_KEY = 'data/API_keys.json'  # Define the S3 key for the API keys JSON file
CHANNEL_IDS_S3_KEY = 'data/channel_ids.csv'  # Define the S3 key for the channel IDs CSV file
RESULTS_S3_KEY = 'data/channel_details/channel_details.csv'  # Define the S3 key to store the results CSV

# Fetch API keys and channel IDs from S3
api_keys = read_api_keys_from_s3(S3_BUCKET, API_KEYS_S3_KEY)  # Read and load API keys from S3
channel_ids_df = read_channel_ids_from_s3(S3_BUCKET, CHANNEL_IDS_S3_KEY)  # Read and load channel IDs from S3

# Fetch channel details using the first API key (for simplicity)
api_key = api_keys[0]  # Use the first API key from the list
results = []  # Initialize an empty list to store channel details
for _, row in channel_ids_df.iterrows():  # Iterate over each row (channel ID) in the DataFrame
    channel_id = row['Channel ID']  # Extract the channel ID from the current row
    details = fetch_channel_details(api_key, channel_id)  # Fetch channel details from YouTube API
    if details:
        results.append(details)  # Append the channel details to the results list

# Convert results to a Pandas DataFrame and CSV
results_df = pd.DataFrame(results)  # Convert the list of channel details to a Pandas DataFrame
csv_buffer = StringIO()  # Create an in-memory string buffer to hold the CSV data
results_df.to_csv(csv_buffer, index=False)  # Write the DataFrame to the buffer in CSV format (without index)

# Upload the results CSV to S3
s3_client.put_object(Bucket=S3_BUCKET, Key=RESULTS_S3_KEY, Body=csv_buffer.getvalue())  # Upload the CSV to S3

print(f'Successfully uploaded channel details CSV to S3 bucket "{S3_BUCKET}" at "{RESULTS_S3_KEY}"')
