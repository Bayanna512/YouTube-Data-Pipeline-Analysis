from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
	'retries': 1,  # 0 for no retries
	"retry_delay": timedelta(minutes=5)
    
}

# Define the DAG
with DAG(
    'youtube_data_extraction_dag',
    default_args=default_args,
    description='DAG to orchestrate AWS Glue jobs for YouTube data extraction and processing',
    schedule_interval=None,  # No automatic scheduling
    catchup=False,
) as dag:

    # Define Glue job tasks
    glue_channel_ids_extraction = GlueJobOperator(
        task_id='glue_channel_ids_extraction',
        job_name='glue_channel_ids_extraction_job',  # Glue job name
        aws_conn_id='aws_default',  # AWS connection ID from airflow
        region_name='us-east-1',  #region 
		schedule_interval='@weekly', # Schedule frequency eg: daily, Weekly, monthly 
    )

    glue_channel_details_extraction = GlueJobOperator(
        task_id='glue_channel_details_extraction',
        job_name='glue_channel_details_extraction_job',  # Glue job name
        aws_conn_id='aws_default',  # AWS connection ID from airflow
        region_name='us-east-1',  #region 
		schedule_interval='@weekly', # Schedule frequency eg: daily, Weekly, monthly 
    )

    glue_video_details_extraction = GlueJobOperator(
        task_id='glue_video_details_extraction',
        job_name='youtube_video_details_job',  # Glue job name
        aws_conn_id='aws_default',  # AWS connection ID from airflow
        region_name='us-east-1',  #region 
		schedule_interval='@weekly', # Schedule frequency eg: daily, Weekly, monthly 
    )

    glue_popular_video_details_extraction = GlueJobOperator(
        task_id='glue_popular_video_details_extraction',
        job_name='glue_popular_video_details_extraction_job',  # Glue job name
        aws_conn_id='aws_default',  # AWS connection ID from airflow
        region_name='us-east-1',  #region 
		schedule_interval='@weekly', # Schedule frequency eg: daily, Weekly, monthly
    )

    # Set up task dependencies
    glue_channel_ids_extraction >> glue_channel_details_extraction >> glue_video_details_extraction >> glue_popular_video_details_extraction
