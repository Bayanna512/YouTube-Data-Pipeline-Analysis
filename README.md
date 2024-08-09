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

Technical Knowledge, Approach, and Algorithms

Technical Challenges
- API Quota Management: Handling API rate limits and quotas by using sequential API keys.


Approach
- Batch Processing: Use AWS Glue for batch processing of data.



Algorithms
- Outlier Detection: Identifying videos with views significantly higher than average.

