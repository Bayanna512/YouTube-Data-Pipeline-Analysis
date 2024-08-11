usecase name :  YouTube channel Data Pipeline 

Overview :

This repository contains a case study for building a data pipeline to ingest, transform, and analyze YouTube data using AWS services.
 The pipeline includes data ingestion from the YouTube API, transformation with AWS Glue, and storage in Amazon Redshift.
 The final reports are generated through SQL queries on Redshift.

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
     

Data Architecture

Architecture Diagram
[Architecture Diagram](docs/architecture_diagram.png)



Data Flow Description
1. Data Ingestion: Glue jobs fetch data from YouTube API and store it in S3 buckets.
2. Data Processing: Glue transformation jobs process the data and load it into Redshift.
3. Data Storage: Data is stored in Redshift for querying and analysis.
4. Data Reporting: SQL queries are run on Redshift to generate reports and insights.

Data Model
- Channel Details Table: `channelid`, `channelname`, `subscribers`, `videocount`
- Video Details Table: `videoid`, `title`, `channelid`, `publishdate`, `viewcount`
- Popular Video Details Table: `videoid`, `title`, `channelid`, `viewcount`, `publishdate`

- ![image](https://github.com/user-attachments/assets/69468e62-34d8-4d15-b8f0-c2e8b865f617)
-![image](https://github.com/user-attachments/assets/24061ed3-8bb0-4d57-aef1-acc3e40d1d27)
-![image](https://github.com/user-attachments/assets/a2a5689c-f560-4f7f-aad7-52498e49c662)




Technical Knowledge, Approach, and Algorithms

The data flow is planned to efficiently fetch video details from the YouTube API, handle pagination, and store the results in S3. The pipeline involves fetching API keys, channel IDs, and video details, processing them, and finally storing them in a Parquet format in S3.
The pipeline is designed to be scalable by using multiple API keys to handle quota limits and employing pagination to fetch large volumes of data. It is maintainable due to its modular functions: fetch_api_keys, fetch_channel_ids, and fetch_videos_with_api_key, which handle distinct tasks.
Data is structured with essential fields like ChannelID, VideoID, Title, PublishDate, and ViewCount. The ingestion pipeline maintains data integrity by processing and storing video details in a structured format.
Data extraction and API interactions occur within the AWS Glue job. Data transformation (e.g., date formatting) is done within the Glue script, while data storage and partitioning are managed when writing to S3 in Parquet format.
Data is partitioned by Year, Month, and Day to optimize query performance and manageability. This partitioning scheme supports efficient querying and retrieval of data.
The code uses VARCHAR for text fields and BIGINT for numeric fields and timestamp for pubshdate in Redshift. Data is stored in Parquet format in S3, which supports efficient compression and columnar storage.
The approach includes handling API quotas by rotating through multiple API keys and managing pagination to ensure all relevant data is fetched. The solution uses AWS Glue for ETL operations, leveraging its integration with Spark for processing and transforming data. The final output is in Parquet format, which is optimized for both storage and query performance.


Algorithms
- Outlier Detection: Identifying videos with views significantly higher than average.

