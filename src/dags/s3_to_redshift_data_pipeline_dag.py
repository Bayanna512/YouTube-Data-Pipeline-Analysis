from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_transformation_load_dag',
    default_args=default_args,
    description='Transform and load data from S3 to Redshift',
    schedule_interval='@daily', #frequency of the DAG
    catchup=False,
) as dag:

    transform_load_channel_details_task = GlueJobOperator(
        task_id='transform_load_channel_details',
        job_name='test_etl1',  # Glue job name for Channel details transformation and load
        aws_conn_id='aws_default', #aws_conn_id from the airflow
        region_name='us-east-1',
    )

    transform_load_video_details_task = GlueJobOperator(
        task_id='transform_load_video_details',
        job_name='t_s3_rs_video_details',  # Glue job name for Video details transformation and load
        aws_conn_id='aws_default', #aws_conn_id from the airflow
        region_name='us-east-1',
    )

    transform_load_popular_video_details_task = GlueJobOperator(
        task_id='transform_load_popular_video_details',
        job_name='t_s3_rs_popular_video_details',  # Glue job name for Popular video details transformation and load
        aws_conn_id='aws_default', #aws_conn_id from the airflow
        region_name='us-east-1',
    )

    # Define task dependencies
    transform_load_channel_details_task >> transform_load_video_details_task >> transform_load_popular_video_details_task

