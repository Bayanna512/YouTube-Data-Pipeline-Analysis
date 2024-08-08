import requests
import sys
import json
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import html

# Retrieve job parameters
# Extracts parameters from the Glue job configuration, allowing you to pass in specific values when the job runs.
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET', 'API_KEY_FILE', 'CHANNEL_DETAILS_FILE', 'start_date', 'end_date'])

# Initialize AWS Glue context
# Initializes a Spark context and a Glue context, which are essential for running Glue jobs.
sc = SparkContext()
glueContext = GlueContext(sc)

# Initialize S3 client
# Sets up an S3 client to interact with AWS S3, allowing you to read from and write to S3 buckets.
s3_client = boto3.client('s3')

def fetch_api_keys(bucket, key):
    # Fetch API keys from the specified S3 bucket and key, allowing you to make requests to the YouTube API.
    response = s3_client.get_object(Bucket=bucket, Key=key)
    api_keys_data = json.loads(response['Body'].read().decode('utf-8'))
    return api_keys_data['VIDEO_API_KEYS']

def fetch_channel_ids(bucket, key):
    # Fetch channel IDs from a CSV file stored in S3, mapping channel names to their corresponding IDs.
    response = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(response['Body'], encoding='utf-8')
    channel_ids = df.set_index('ChannelName')['ChannelID'].to_dict()
    return channel_ids

def fetch_videos_with_api_key(channel_id, api_key, start_date, end_date, max_videos, next_page_token=None, fetched_video_ids=None):
    # Fetches videos from a given channel using the YouTube API, handling pagination and API quota limits.
    # The function supports fetching up to `max_videos` videos and returns a list of video data.
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
                # Handle quota exceeded error by switching to the next API key.
                print(f"Quota exceeded for API key {api_key}. Moving to the next API key.")
                return videos, True, fetched_video_ids, next_page_token

            # Print other errors and break the loop.
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
                # Print errors when fetching video details.
                print(f"Error fetching video details: {video_data['error']['message']}")
                continue

            video_title = html.unescape(video_title)

            video_stats = video_data.get('items', [{}])[0].get('statistics', {})
            view_count = int(video_stats.get('viewCount', 0))
            
            # Ensure publish_date exists before appending to the list.
            if publish_date:
                videos.append({
                    'ChannelID': channel_id,
                    'VideoID': video_id,
                    'Title': video_title,
                    'PublishDate': publish_date,
                    'ViewCount': view_count
                })
            else:
                # Warn if publish date is missing, and skip this video.
                print(f"Warning: Missing PublishDate for video {video_id} on channel {channel_id}")

            fetched_video_ids.add(video_id)
            fetched_count += 1

        # Handle pagination using the next page token.
        next_page_token = data.get('nextPageToken')
        if next_page_token and fetched_count < max_videos:
            url = f'{base_url}&pageToken={next_page_token}'
        else:
            url = None

    return videos, not next_page_token, fetched_video_ids, next_page_token

def fetch_and_store_youtube_data(start_date, end_date):
    # This function coordinates the fetching and storing of YouTube video data for all channels and API keys.
    # It also handles data transformation and saving the data into S3 in a structured format.
    S3_BUCKET = args['S3_BUCKET']
    API_KEY_FILE = args['API_KEY_FILE']
    CHANNEL_DETAILS_FILE = args['CHANNEL_DETAILS_FILE']

    # Fetch API keys and channel details from S3.
    api_keys = fetch_api_keys(S3_BUCKET, API_KEY_FILE)
    channel_ids = fetch_channel_ids(S3_BUCKET, CHANNEL_DETAILS_FILE)

    all_video_data = []
    max_videos_per_key = 500  # Maximum videos to fetch per API key to avoid quota issues.

    fetched_video_ids = set()

    for channel_name, channel_id in channel_ids.items():
        api_key_index = 0
        next_page_token = None

        while api_key_index < len(api_keys):
            api_key = api_keys[api_key_index]
            try:
                # Fetch videos for the channel using the current API key.
                videos, quota_exceeded, fetched_video_ids, next_page_token = fetch_videos_with_api_key(channel_id, api_key, start_date, end_date, max_videos_per_key, next_page_token, fetched_video_ids)
                all_video_data.extend(videos)

                if quota_exceeded:
                    # If quota is exceeded, move to the next API key.
                    api_key_index += 1
                    continue

                break
            except Exception as e:
                # Handle any exceptions that occur during the video fetching process.
                print(f"Exception occurred while processing channel {channel_id} with API key {api_key}: {str(e)}")
                api_key_index += 1

    df = pd.DataFrame(all_video_data)
    df['PublishDate'] = pd.to_datetime(df['PublishDate'], errors='coerce')
    df['year'] = df['PublishDate'].dt.year
    df['month'] = df['PublishDate'].dt.month
    df['day'] = df['PublishDate'].dt.day

    # Drop rows where PublishDate could not be converted.
    df = df.dropna(subset=['PublishDate'])

    # Group data by year, month, and day, and store each group as a separate CSV file in S3.
    for (year, month, day), group_df in df.groupby(['year', 'month', 'day']):
        s3_path = f'data/video_details/year={year}/month={month:02d}/day={day:02d}/'
        csv_buffer = StringIO()
        group_df.drop(columns=['year', 'month', 'day']).to_csv(csv_buffer, index=False, encoding='utf-8')
        s3_client.put_object(Bucket=S3_BUCKET, Key=f'{s3_path}video_data_{start_date}_{end_date}.csv', Body=csv_buffer.getvalue())

# Start and end dates from job parameters
start_date = args['start_date']
end_date = args['end_date']
fetch_and_store_youtube_data(start_date, end_date)
