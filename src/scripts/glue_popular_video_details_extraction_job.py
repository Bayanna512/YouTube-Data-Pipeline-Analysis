import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import requests
import json
import boto3

# Initialize Glue context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# S3 bucket details
s3_bucket = 'sphtest512'
channel_details_path = f's3://{s3_bucket}/data/channel_details.csv'
api_keys_path = f's3://{s3_bucket}/data/API_keys.json'

# Read channel details from S3
channel_details_df = spark.read.csv(channel_details_path, header=True)

# Debugging: Print the schema and data of the DataFrame
channel_details_df.printSchema()
channel_details_df.show(5)

# Extract channel IDs
channel_ids = [row['Channel ID'] for row in channel_details_df.collect()]

# Read API keys from S3
s3_client = boto3.client('s3')
api_keys_object = s3_client.get_object(Bucket=s3_bucket, Key='data/API_keys.json')
api_keys_content = api_keys_object['Body'].read().decode('utf-8')
api_keys = json.loads(api_keys_content)['VIDEO_API_KEYS']

# Function to fetch top 50 videos for a given channel ID
def fetch_top_videos(channel_id, api_key):
    YOUTUBE_API_SEARCH_URL = 'https://www.googleapis.com/youtube/v3/search'
    YOUTUBE_API_VIDEOS_URL = 'https://www.googleapis.com/youtube/v3/videos'
    
    # Fetching video IDs
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
        videos = search_response.json().get('items', [])
        video_ids = [video['id']['videoId'] for video in videos]
        
        # Fetching video statistics and publish date
        videos_params = {
            'part': 'statistics,snippet',
            'id': ','.join(video_ids),
            'key': api_key
        }
        videos_response = requests.get(YOUTUBE_API_VIDEOS_URL, params=videos_params)
        
        if videos_response.status_code == 200:
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

# Initialize a list to store video data
video_data = []

# Loop through each channel ID and fetch the top 50 videos
for api_key in api_keys:
    for channel_id in channel_ids:
        video_data.extend(fetch_top_videos(channel_id, api_key))

# Create DataFrame
columns = ['ChannelID', 'VideoID', 'Title', 'ViewCount', 'PublishDate']
df = spark.createDataFrame(video_data, columns)

# Show the top 50 videos
df.show(50, truncate=False)

# Save the DataFrame to S3 as a single CSV file
output_path = f's3://{s3_bucket}/data/popular_video_details/'
df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)

# Stop the Spark session
spark.stop()

