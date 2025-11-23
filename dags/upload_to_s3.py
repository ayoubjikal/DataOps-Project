# dags/load_s3_to_snowflake.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
import snowflake.connector

default_args = {
    "owner": "ayoub",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}
def creat_warehouse():
    return " CREATE WAREHOUSE IF NOT EXISTS ecommerce_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;"

def upload_snowflake():
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
    cur.execute("  COPY INTO ecommerce_table FROM @ecommerce_stage FILE_FORMAT = my_csv_format")
    print(cur.fetchone())
    sf.close()

with DAG(
    dag_id="load_s3_to_snowflake",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    tags=["load", "snowflake", "s3"],
) as dag:

    t1 = PythonOperator(task_id='test_snowflake', python_callable=upload_snowflake)

