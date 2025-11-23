from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import snowflake.connector
import boto3

def test_snowflake():
    conn = BaseHook.get_connection("snowflake_conn")
    sf = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=conn.extra_dejson.get("account"),
        warehouse=conn.extra_dejson.get("warehouse"),
        database=conn.extra_dejson.get("database"),
        schema=conn.schema,
        role=conn.extra_dejson.get("role")
    )
    cur = sf.cursor()
    cur.execute("SELECT CURRENT_DATE;")
    print(cur.fetchone())
    sf.close()

def test_aws():
    conn = BaseHook.get_connection("aws_default")
    s3 = boto3.client(
        's3',
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=conn.extra_dejson.get("region_name")
    )
    buckets = s3.list_buckets()
    print("Buckets:", [b['Name'] for b in buckets['Buckets']])

with DAG('test_connections', start_date=datetime(2025,11,22), schedule=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='test_snowflake', python_callable=test_snowflake)
    t2 = PythonOperator(task_id='test_aws', python_callable=test_aws)
