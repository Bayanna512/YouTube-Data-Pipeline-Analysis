import requests
import sys
import json
import pandas as pd
import boto3
from io import BytesIO
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import html
import pyarrow as pa
import pyarrow.parquet as pq

# Retrieve job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'API_KEY_FILE', 'CHANNEL_DETAILS_FILE', 'start_date', 'end_date'])

# Initialize AWS Glue context
sc = SparkContext()
glueContext = GlueContext(sc)

# Initialize S3 client
s3_client = boto3.client('s3')

def fetch_api_keys(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    api_keys_data = json.loads(response['Body'].read().decode('utf-8'))
    return api_keys_data['VIDEO_API_KEYS']

def fetch_channel_ids(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(response['Body'], encoding='utf-8')
    channel_ids = df.set_index('ChannelName')['ChannelID'].to_dict()
    return channel_ids

def fetch_videos_with_api_key(channel_id, api_key, start_date, end_date, max_videos, next_page_token=None, fetched_video_ids=None):
    if fetched_video_ids is None:
        fetched_video_ids = set()

    videos = []
    base_url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&channelId={channel_id}&order=date&key={api_key}&publishedAfter={start_date}T00:00:00Z&publishedBefore={end_date}T23:59:59Z'
    url = base_url if not next_page_token else f'{base_url}&pageToken={next_page_token}'
    fetched_count = 0

    while url and fetched_count < max_videos:
        response = requests.get(url)
        data = response.json()

        if 'error' in data:
            if data['error']['errors'][0]['reason'] == 'quotaExceeded':
                print(f"Quota exceeded for API key {api_key}. Moving to the next API key.")
                return videos, True, fetched_video_ids, next_page_token

            print(f"Error fetching data: {data['error']['message']}")
            break

        for item in data.get('items', []):
            if fetched_count >= max_videos:
                break
            video_id = item['id']['videoId']
            if video_id in fetched_video_ids:
                continue

            video_title = item['snippet']['title']
            publish_date = item['snippet'].get('publishedAt')  # Use get() to avoid KeyError
            video_details_url = f'https://www.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={api_key}'
            video_response = requests.get(video_details_url)
            video_data = video_response.json()

            if 'error' in video_data:
                print(f"Error fetching video details: {video_data['error']['message']}")
                continue

            video_title = html.unescape(video_title)

            video_stats = video_data.get('items', [{}])[0].get('statistics', {})
            view_count = int(video_stats.get('viewCount', 0))
            
            # Ensure publish_date exists before appending to the list
            if publish_date:
                videos.append({
                    'ChannelID': channel_id,
                    'VideoID': video_id,
                    'Title': video_title,
                    'PublishDate': publish_date,
                    'ViewCount': view_count
                })
            else:
                print(f"Warning: Missing PublishDate for video {video_id} on channel {channel_id}")

            fetched_video_ids.add(video_id)
            fetched_count += 1

        next_page_token = data.get('nextPageToken')
        if next_page_token and fetched_count < max_videos:
            url = f'{base_url}&pageToken={next_page_token}'
        else:
            url = None

    return videos, not next_page_token, fetched_video_ids, next_page_token

def fetch_and_store_youtube_data(start_date, end_date):
    S3_BUCKET = args['S3_BUCKET']
    API_KEY_FILE = args['API_KEY_FILE']
    CHANNEL_DETAILS_FILE = args['CHANNEL_DETAILS_FILE']

    # Fetch API keys and channel details
    api_keys = fetch_api_keys(S3_BUCKET, API_KEY_FILE)
    channel_ids = fetch_channel_ids(S3_BUCKET, CHANNEL_DETAILS_FILE)

    all_video_data = []
    max_videos_per_key = 500

    fetched_video_ids = set()

    for channel_name, channel_id in channel_ids.items():
        api_key_index = 0
        next_page_token = None

        while api_key_index < len(api_keys):
            api_key = api_keys[api_key_index]
            try:
                videos, quota_exceeded, fetched_video_ids, next_page_token = fetch_videos_with_api_key(channel_id, api_key, start_date, end_date, max_videos_per_key, next_page_token, fetched_video_ids)
                all_video_data.extend(videos)

                if quota_exceeded:
                    api_key_index += 1
                    continue

                break
            except Exception as e:
                print(f"Exception occurred while processing channel {channel_id} with API key {api_key}: {str(e)}")
                api_key_index += 1

    df = pd.DataFrame(all_video_data)
    df['PublishDate'] = pd.to_datetime(df['PublishDate'], errors='coerce')
    df = df.dropna(subset=['PublishDate'])

    # Convert the entire DataFrame to a Parquet file and upload to S3
    parquet_buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_buffer)

    # Upload to S3
    s3_client.put_object(Bucket=S3_BUCKET, Key=f'data/video_details/video_data_{start_date}_{end_date}.parquet', Body=parquet_buffer.getvalue())

# Start and end dates from job parameters
start_date = args['start_date']
end_date = args['end_date']
fetch_and_store_youtube_data(start_date, end_date)
