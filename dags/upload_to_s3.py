from airflow import DAG
# from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator


with DAG(
    dag_id='upload_raw_to_s3',
    # schedule_interval=None,
    # start_date = days_ago(1)

    ) as dag:
    
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename="/usr/local/airflow/include/data/data.csv",
        dest_key="raw/",
        dest_bucket="ecommerce-dataops",
        aws_conn_id='aws_default', 
        
    )
    
    
    upload_to_s3