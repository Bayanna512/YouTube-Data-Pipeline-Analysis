import requests  # For making HTTP requests to the YouTube Data API
import sys  # For interacting with the system, particularly for command-line arguments
import json  # For parsing JSON data
import boto3  # AWS SDK for Python to interact with AWS services like S3
from datetime import datetime  # For handling dates and times
from awsglue.utils import getResolvedOptions  # For retrieving job parameters in AWS Glue
from pyspark.context import SparkContext  # For initializing the Spark context
from awsglue.context import GlueContext  # For initializing the AWS Glue context
from pyspark.sql import SparkSession  # For creating and managing Spark SQL DataFrames
from pyspark.sql.functions import col, to_timestamp, lit  # For manipulating DataFrame columns
import html  # For HTML escaping/decoding
from io import BytesIO  # For handling byte streams

# Retrieve job parameters passed to the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'API_KEY_FILE', 'CHANNEL_DETAILS_FILE', 'start_date', 'end_date'])

# Initialize Spark and Glue contexts
sc = SparkContext()  # Creates a SparkContext for Spark operations
glueContext = GlueContext(sc)  # Creates a GlueContext for Glue-specific operations
spark = glueContext.spark_session  # Creates a SparkSession to work with Spark SQL

# Initialize S3 client to interact with AWS S3
s3_client = boto3.client('s3')

def fetch_api_keys(bucket, key):
 def fetch_api_keys(bucket, key):
    """Fetches API keys from a CSV file in the S3 bucket."""
    s3_path = f"s3://{bucket}/{key}"  # Construct the S3 URI for the file

    # Load CSV data into a Spark DataFrame
    spark_df = spark.read.csv(s3_path, header=True, inferSchema=True)

    # Collect API keys into a list
    api_keys = spark_df.select("APIKey").rdd.flatMap(lambda x: x).collect()

    return api_keys  # Returns the list of API keys

def fetch_channel_ids(bucket, key):
    """Fetches channel IDs from a CSV file in the S3 bucket."""
    s3_path = f"s3://{bucket}/{key}"  # Construct the S3 URI for the file

    # Load CSV data directly into a Spark DataFrame
    spark_df = spark.read.csv(s3_path, header=True, inferSchema=True)

    # Collect channel IDs into a dictionary
    channel_ids = spark_df.select("ChannelName", "ChannelID").rdd.collectAsMap()

    return channel_ids  # Returns the dictionary of channel names and IDs

def fetch_videos_with_api_key(channel_id, api_key, start_date, end_date, max_videos, next_page_token=None, fetched_video_ids=None):
    """Fetches video details from YouTube API for a given channel and API key."""
    if fetched_video_ids is None:
        fetched_video_ids = set()  # Initialize set for tracking fetched video IDs

    videos = []  # List to store video details
    base_url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&channelId={channel_id}&order=date&key={api_key}&publishedAfter={start_date}T00:00:00Z&publishedBefore={end_date}T23:59:59Z'
    url = base_url if not next_page_token else f'{base_url}&pageToken={next_page_token}'
    fetched_count = 0  # Counter for fetched videos

    while url and fetched_count < max_videos:
        response = requests.get(url)  # Make a request to the YouTube API
        data = response.json()  # Parse the JSON response

        if 'error' in data:
            if data['error']['errors'][0]['reason'] == 'quotaExceeded':
                print(f"Quota exceeded for API key {api_key}. Moving to the next API key.")
                return videos, True, fetched_video_ids, next_page_token  # Return with quota exceeded flag

            print(f"Error fetching data: {data['error']['message']}")
            break  # Exit loop on error

        for item in data.get('items', []):
            if fetched_count >= max_videos:
                break
            video_id = item['id']['videoId']
            if video_id in fetched_video_ids:
                continue  # Skip if video already fetched

            video_title = item['snippet']['title']
            publish_date = item['snippet'].get('publishedAt')
            video_details_url = f'https://www.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={api_key}'
            video_response = requests.get(video_details_url)
            video_data = video_response.json()

            if 'error' in video_data:
                print(f"Error fetching video details: {video_data['error']['message']}")
                continue  # Skip if there is an error fetching video details

            video_title = html.unescape(video_title)  # Decode HTML entities in title
            video_stats = video_data.get('items', [{}])[0].get('statistics', {})
            view_count = int(video_stats.get('viewCount', 0))

            if publish_date:
                videos.append({
                    'ChannelID': channel_id,
                    'VideoID': video_id,
                    'Title': video_title,
                    'PublishDate': publish_date,
                    'ViewCount': view_count
                })

            fetched_video_ids.add(video_id)  # Add video ID to the set
            fetched_count += 1  # Increment counter

        next_page_token = data.get('nextPageToken')  # Get the next page token for pagination
        if next_page_token and fetched_count < max_videos:
            url = f'{base_url}&pageToken={next_page_token}'
        else:
            url = None

    return videos, not next_page_token, fetched_video_ids, next_page_token

def fetch_and_store_youtube_data(start_date, end_date):
    """Fetches video data from YouTube and stores it in S3 in Parquet format."""
    S3_BUCKET = args['S3_BUCKET']
    API_KEY_FILE = args['API_KEY_FILE']
    CHANNEL_DETAILS_FILE = args['CHANNEL_DETAILS_FILE']

    # Fetch API keys and channel details
    api_keys = fetch_api_keys(S3_BUCKET, API_KEY_FILE)
    channel_ids = fetch_channel_ids(S3_BUCKET, CHANNEL_DETAILS_FILE)

    all_video_data = []  # List to store all video data
    max_videos_per_key = 5000  # Maximum number of videos to fetch per API key
    fetched_video_ids = set()  # Set to track fetched video IDs

    for channel_name, channel_id in channel_ids.items():
        api_key_index = 0
        next_page_token = None

        while api_key_index < len(api_keys):
            api_key = api_keys[api_key_index]
            try:
                videos, quota_exceeded, fetched_video_ids, next_page_token = fetch_videos_with_api_key(channel_id, api_key, start_date, end_date, max_videos_per_key, next_page_token, fetched_video_ids)
                all_video_data.extend(videos)  # Add fetched videos to the list

                if quota_exceeded:
                    api_key_index += 1  # Move to the next API key
                    continue

                break
            except Exception as e:
                print(f"Exception occurred while processing channel {channel_id} with API key {api_key}: {str(e)}")
                api_key_index += 1  # Move to the next API key

    # Convert the list of video data to a Spark DataFrame
    df = spark.createDataFrame(all_video_data)
    df = df.withColumn('PublishDate', to_timestamp(col('PublishDate')))  # Convert 'PublishDate' to timestamp format

    # Extract year, month, and day for partitioning
    df = df.withColumn('Year', col('PublishDate').substr(1, 4))
    df = df.withColumn('Month', col('PublishDate').substr(6, 2))
    df = df.withColumn('Day', col('PublishDate').substr(9, 2))

    # Write the DataFrame to S3 in Parquet format, partitioned by Year, Month, and Day
    df.write.partitionBy('Year', 'Month', 'Day').parquet(f"s3://{S3_BUCKET}/data/video_details/", mode='overwrite')

# Start and end dates from job parameters
start_date = args['start_date']
end_date = args['end_date']
fetch_and_store_youtube_data(start_date, end_date)  # Call the function to fetch and store data
