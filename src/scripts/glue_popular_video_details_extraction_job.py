import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import requests
import json
import boto3

# Initialize Glue context for using AWS Glue's Spark environment
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Define S3 bucket and file paths
s3_bucket = 'sphtest512'
channel_details_path = f's3://{s3_bucket}/data/channel_details.csv'
api_keys_path = f's3://{s3_bucket}/data/API_keys.json'

# Read channel details from the CSV file in S3 into a Spark DataFrame
channel_details_df = spark.read.csv(channel_details_path, header=True)

# Debugging: Print the schema and the first 5 rows of the DataFrame to verify the data
channel_details_df.printSchema()
channel_details_df.show(5)

# Extract channel IDs from the DataFrame into a list
channel_ids = [row['Channel ID'] for row in channel_details_df.collect()]

# Initialize an S3 client to interact with S3 service
s3_client = boto3.client('s3')

# Read API keys from the JSON file in S3
api_keys_object = s3_client.get_object(Bucket=s3_bucket, Key='data/API_keys.json')
api_keys_content = api_keys_object['Body'].read().decode('utf-8')
api_keys = json.loads(api_keys_content)['VIDEO_API_KEYS']

# Function to fetch the top 50 most viewed videos for a given YouTube channel
def fetch_top_videos(channel_id, api_key):
    YOUTUBE_API_SEARCH_URL = 'https://www.googleapis.com/youtube/v3/search'
    YOUTUBE_API_VIDEOS_URL = 'https://www.googleapis.com/youtube/v3/videos'
    
    # Parameters to search for videos in the channel, ordered by view count
    search_params = {
        'part': 'snippet',
        'channelId': channel_id,
        'maxResults': 50,
        'order': 'viewCount',
        'type': 'video',
        'key': api_key
    }
    search_response = requests.get(YOUTUBE_API_SEARCH_URL, params=search_params)
    
    if search_response.status_code == 200:
        # Extract video IDs from the search response
        videos = search_response.json().get('items', [])
        video_ids = [video['id']['videoId'] for video in videos]
        
        # Parameters to fetch detailed statistics and publish date for the videos
        videos_params = {
            'part': 'statistics,snippet',
            'id': ','.join(video_ids),
            'key': api_key
        }
        videos_response = requests.get(YOUTUBE_API_VIDEOS_URL, params=videos_params)
        
        if videos_response.status_code == 200:
            # Extract and format the necessary video details
            video_statistics = videos_response.json().get('items', [])
            return [
                (
                    channel_id,
                    video['id'],
                    next(v['snippet']['title'] for v in videos if v['id']['videoId'] == video['id']),
                    int(video['statistics'].get('viewCount', 0)),
                    video['snippet']['publishedAt']
                ) for video in video_statistics
            ]
        else:
            videos_response.raise_for_status()
    else:
        search_response.raise_for_status()

# Initialize a list to store the fetched video data
video_data = []

# Loop through each API key and channel ID to fetch the top 50 videos per channel
for api_key in api_keys:
    for channel_id in channel_ids:
        video_data.extend(fetch_top_videos(channel_id, api_key))

# Define the schema for the DataFrame columns
columns = ['ChannelID', 'VideoID', 'Title', 'ViewCount', 'PublishDate']

# Create a Spark DataFrame from the fetched video data
df = spark.createDataFrame(video_data, columns)

# Display the top 50 videos without truncating the text fields
df.show(50, truncate=False)

# Save the DataFrame as a CSV file to the specified S3 path, coalescing into a single file
output_path = f's3://{s3_bucket}/data/popular_video_details/'
df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)

# Stop the Spark session to free up resources
spark.stop()
