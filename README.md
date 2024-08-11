usecase name :  YouTube channel Data Pipeline 

Overview :

This repository contains a case study for building a data pipeline to ingest, transform, and analyze YouTube data using AWS services.
 The pipeline includes data ingestion from the YouTube API, transformation with AWS Glue, and storage in Amazon Redshift.
 The final reports are generated through SQL queries on Redshift.

 Repository Structure:

 /YouTube-Data-Pipeline-Analysis
│
├── /docs
│   ├── data_model_diagram.pdf
│   └── README.md
│
├── /src
│   ├── /dags
│   │   ├── youtube_data_extraction_dag.py
│   │   ├── s3_to_redshift_data_pipeline_dag.py
│   │
│   ├── /scripts
│   │   ├── glue_channel_ids_extraction_job.py
│   │   ├── glue_channel_details_extraction_job.py
│   │   ├── youtube_video_details_job.py
│   │   ├── glue_popular_video_details_extraction_job.py
│   │   ├── test_etl1.json
│   │   ├── t_s3_rs_video_details.json
│   │   ├── t_s3_rs_popular_video_details.json
│   │
│   └── /configs
│       └── api_keys.json
│
├── /data
│   ├── /s3
│   │   ├── channel_ids.csv
│   │   ├── channel_details.csv
│   │   ├── video_details.parquet
│   │   └── popular_video_details.csv
│   │
│   └── /redshift
│       ├── channel_details_table.sql
│       ├── video_details_table.sql
│       ├── popular_video_details_table.sql
│       ├── reporting_queries.sql
│       └── /results
│           └── result.xlsx
│
└── README.md


Data Pipeline Solution :

1. Data Pipeline Overview
- Ingestion: Fetch data from YouTube API and store it in S3.
- Transformation: Use AWS Glue to transform data from S3 into Redshift.
- Storage: Load data into Amazon Redshift for querying and analysis.
- Reporting: Execute SQL queries on Redshift to generate insights.

2. Glue Jobs and Their Names
- Channel IDs Extraction Job: `glue_channel_ids_extraction_job`
- Channel Details Extraction Job: `glue_channel_details_extraction_job`
- Video Details Extraction Job: `youtube_video_details_job`
- Popular Video Details Extraction Job: `glue_popular_video_details_extraction_job`

3. Data Transformation and Load Jobs
- Channel details (S3 to Redshift): `test_etl1`
- Video Details (S3 to Redshift): `t_s3_rs_video_details`
- Popular Video Details (S3 to Redshift): `t_s3_rs_popular_video_details`

  4) DAGs :
     - youtube_data_extraction_dag
     - s3_to_redshift_data_pipeline_dag
     


Data Flow Description
1. Data Ingestion: Glue jobs fetch data from YouTube API and store it in S3 buckets.
2. Data Processing: Glue transformation jobs process the data and load it into Redshift.
3. Data Storage: Data is stored in Redshift for querying and analysis.
4. Data Reporting: SQL queries are run on Redshift to generate reports and insights.

Data Model
- Channel Details Table: `channelid`, `channelname`, `subscribers`, `videocount`
- Video Details Table: `videoid`, `title`, `channelid`, `publishdate`, `viewcount`
- Popular Video Details Table: `videoid`, `title`, `channelid`, `viewcount`, `publishdate`

Channel Details			
Column Name	Data Type	Description	keys
channelid	VARCHAR(50)	Unique identifier for the channel.	Primary Key
channelname	VARCHAR(255)	Name of the channel.	
subscribercount	BIGINT	Number of subscribers.	
videocount	BIGINT	Total number of videos.	
			
			
			
Video Details			
Column Name	Data Type	Description	Notes
videoid	VARCHAR(255)	Unique identifier for the video.	Primary Key
channelid	VARCHAR(255)	Foreign key referencing channelid from channel_details.	Foreign Key
title	VARCHAR(1000)	Title of the video.	
publishdate	TIMESTAMP	Date and time when the video was published.	
viewcount	BIGINT	Number of views the video has received.	
			
			
Popular Video Details			
Column Name	Data Type	Description	Notes
videoid	VARCHAR(255)	Unique identifier for the video.	Primary Key
channelid	VARCHAR(255)	Foreign key referencing channelid from channel_details.	Foreign Key
title	VARCHAR(1000)	Title of the video.	
viewcount	BIGINT	Number of views the video has received.	
publishdate	TIMESTAMP	Date and time when the video was published.	
publishdate_string	VARCHAR(65535)	String representation of the publish date.	
viewcount_string	VARCHAR(65535)	String representation of the view count.	
![image](https://github.com/user-attachments/assets/bbe747ee-f8c3-4482-a338-5fe1524fe9a4)



Technical Knowledge, Approach, and Algorithms

The data flow is planned to efficiently fetch video details from the YouTube API, handle pagination, and store the results in S3. The pipeline involves fetching API keys, channel IDs, and video details, processing them, and finally storing them in a Parquet format in S3.
The pipeline is designed to be scalable by using multiple API keys to handle quota limits and employing pagination to fetch large volumes of data. It is maintainable due to its modular functions: fetch_api_keys, fetch_channel_ids, and fetch_videos_with_api_key, which handle distinct tasks.
Data is structured with essential fields like ChannelID, VideoID, Title, PublishDate, and ViewCount. The ingestion pipeline maintains data integrity by processing and storing video details in a structured format.
Data extraction and API interactions occur within the AWS Glue job. Data transformation (e.g., date formatting) is done within the Glue script, while data storage and partitioning are managed when writing to S3 in Parquet format.
Data is partitioned by Year, Month, and Day to optimize query performance and manageability. This partitioning scheme supports efficient querying and retrieval of data.
The code uses VARCHAR for text fields and BIGINT for numeric fields and timestamp for pubshdate in Redshift. Data is stored in Parquet format in S3, which supports efficient compression and columnar storage.
The approach includes handling API quotas by rotating through multiple API keys and managing pagination to ensure all relevant data is fetched. The solution uses AWS Glue for ETL operations, leveraging its integration with Spark for processing and transforming data. The final output is in Parquet format, which is optimized for both storage and query performance.
