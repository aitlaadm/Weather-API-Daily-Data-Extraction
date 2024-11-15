from datetime import datetime, timedelta
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.connection import Connection
# from airflow.operators.python import PythonOperator
from airflow import DAG
import os

SPARK_HOME=os.getenv("SPARK_HOME")

default_args={
    'owner': "Simo",
    'retries':5,
    'start_date': datetime(2024,11,5),
    'retry_delay': timedelta(minutes=5)
}

c= Connection(
    conn_id="mysql_connection",
    conn_type="mysql",
    description="Connecte to create table and insert metz weather values",
    host="localhost",
    login="simo",
    password="simointhehouse",
)

with DAG (
    'api_test_v1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # Define the Spark submit task
    submit_job = SparkSubmitOperator(
        application=f"{SPARK_HOME}/jobs/pysparksubmitoperator.py",  # Path accessible by Spark
        task_id="spark-submit",
        conn_id="spark_default",  # Connection to Spark cluster
        conf={
            'spark.master': 'spark://spark-master:7077',  # Ensures it connects to the Docker Spark cluster
            'spark.executor.memory': '2g',
            'spark.executor.cores': '1'
        },
    )
submit_job