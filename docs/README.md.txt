# YouTube Data Pipeline Case Study

## Overview

This repository contains a case study for building a data pipeline to ingest, transform, and analyze YouTube data using AWS services. The pipeline includes data ingestion from the YouTube API, transformation with AWS Glue, and storage in Amazon Redshift. The final reports are generated through SQL queries on Redshift.

## Data Pipeline Solution

### 1. Data Pipeline Overview
- **Ingestion**: Fetch data from YouTube API and store it in S3.
- **Transformation**: Use AWS Glue to transform data from S3 into Redshift.
- **Storage**: Load data into Amazon Redshift for querying and analysis.
- **Reporting**: Execute SQL queries on Redshift to generate insights.

### 2. Glue Jobs and Their Names
- **Channel IDs Extraction Job**: `glue_channel_ids_extraction_job`
- **Channel Details Extraction Job**: `glue_channel_details_extraction_job`
- **Video Details Extraction Job**: `glue_video_details_extraction_job`
- **Popular Video Details Extraction Job**: `glue_popular_video_details_extraction_job`

### 3. Data Transformation and Load Jobs
- **Channel IDs (S3 to Redshift)**: `glue_transform_load_channel_ids`
- **Video Details (S3 to Redshift)**: `glue_transform_load_video_details`
- **Popular Video Details (S3 to Redshift)**: `glue_transform_load_popular_video_details`

## Data Architecture

### Architecture Diagram
![Architecture Diagram](docs/architecture_diagram.png)

### Data Flow Description
1. **Data Ingestion**: Glue jobs fetch data from YouTube API and store it in S3 buckets.
2. **Data Processing**: Glue transformation jobs process the data and load it into Redshift.
3. **Data Storage**: Data is stored in Redshift for querying and analysis.
4. **Data Reporting**: SQL queries are run on Redshift to generate reports and insights.

### Data Model
- **Channel Details Table**: `channelid`, `channelname`, `subscribers`, `videocount`
- **Video Details Table**: `videoid`, `title`, `channelid`, `publishdate`, `viewcount`
- **Popular Video Details Table**: `videoid`, `title`, `channelid`, `viewcount`, `publishdate`

## Technical Knowledge, Approach, and Algorithms

### Technical Challenges
- **API Quota Management**: Handling API rate limits and quotas by using sequential API keys.
- **Data Volume**: Efficiently processing large volumes of data from YouTube.

### Approach
- **Batch Processing**: Use AWS Glue for batch processing of data.
- **Optimized Storage**: Store data in a columnar format (e.g., Parquet) for efficient querying.

### Algorithms
- **Trend Analysis**: Aggregating video counts by month.
- **Outlier Detection**: Identifying videos with views significantly higher than average.

## Analysis and Findings

### SQL Queries and Results
1. **Subscribers Count**:
    ```sql
    SELECT channel_name, subscribers
    FROM channel_details;
    ```

2. **Videos Published**:
    ```sql
    SELECT channelname, videocount
    FROM channel_details;
    ```

3. **Publishing Trend**:
    ```sql
    SELECT c.channelname, TO_CHAR(DATE_TRUNC('month', publishdate), 'Month YYYY') AS month, COUNT(v.videoid) AS videocount
    FROM video_details v
    JOIN channel_details c ON v.channelid = c.channelid
    WHERE v.publishdate >= DATEADD(month, -12, CURRENT_DATE)
    GROUP BY c.channelname, DATE_TRUNC('month', v.publishdate)
    ORDER BY c.channelname, month;
    ```

4. **Most Viewed Videos**:
    ```sql
    SELECT DISTINCT c.channelname, pvd.title, pvd.viewcount, pvd.publishdate
    FROM popular_video_details pvd
    JOIN channel_details c ON pvd.channelid = c.channelid
    ORDER BY viewcount DESC;
    ```

5. **Channel Comparison**:
    ```sql
    SELECT c.channelname, AVG(viewcount) AS avg_views_per_video
    FROM channel_details c
    JOIN video_details v ON v.channelid = c.channelid
    GROUP BY c.channelname
    ORDER BY avg_views_per_video DESC;
    ```

6. **Outliers and Anomalies**:
    ```sql
    SELECT DISTINCT c.channelname, v.title, v.viewcount
    FROM channel_details c
    JOIN video_details v ON v.channelid = c.channelid
    WHERE v.viewcount > (SELECT AVG(viewcount) + 2 * STDDEV(viewcount) FROM video_details)
    ORDER BY v.viewcount DESC;
    ```

## Recommendations for Improvements

### Future Enhancements
- **Real-time Data**: Implement real-time data ingestion and processing.
- **Advanced Analytics**: Use machine learning to predict trends and analyze audience sentiment.

### Best Practices
- **Data Management**: Ensure data is well-structured and partitioned for efficient querying.
- **Security**: Implement appropriate access controls and encryption for data stored in S3 and Redshift.

## Code Documentation

### Code Overview
- **Ingestion Scripts**: Scripts used to fetch data from YouTube API and load it into S3.
- **Transformation Scripts**: Glue scripts used to transform and load data from S3 to Redshift.

### Code Snippets
- **Ingestion**: Example code snippets for Glue jobs that fetch and store data.
- **Transformation**: Code snippets for data transformation and loading.

### Configuration Details
- **API Keys**: Details on how API keys are managed and used.
- **Glue Job Configurations**: Descriptions of Glue job configurations, including sources and targets.

## Presentation Deck
- [Presentation Deck](docs/presentation_deck.pdf)

## Contributing
If you have suggestions or improvements, feel free to create an issue or submit a pull request.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.