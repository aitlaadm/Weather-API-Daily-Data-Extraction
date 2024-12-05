from datetime import datetime, timedelta
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from Cassandra.cluster import Cluster
from airflow import DAG
import os

SPARK_HOME=os.getenv("SPARK_HOME")

default_args={
    'owner': "Simo",
    'retries':3,
    'start_date': datetime(2024,11,5),
    'retry_delay': timedelta(minutes=1)
}
def dynamic_callable(script_path, **kwargs):
    with open(script_path, 'r') as file:
        script_content = file.read()
    # Execute the script content dynamically
    exec(script_content)


with DAG (
    'api_test_v2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    
    create_cassandra_keyspace_tables=PythonOperator(
        task_id="create_cassandra_keyspace_tables",
        python_callable=dynamic_callable,
        op_kwargs={'script_path':f'{SPARK_HOME}/jobs/create_namespaces_and_tables.py'}
    )
    # Define the Spark submit task
    ingest_meteo_job = SparkSubmitOperator(
        application=f"{SPARK_HOME}/jobs/pysparksubmitoperator.py",  # Path accessible by Spark
        task_id="ingest_meteo_job",
        conn_id="spark_default",  # Connection to Spark cluster
        packages='com.datastax.spark:spark-cassandra-connector_2.12:3.4.0',
        verbose=True
    )
    ingest_pollution_job = SparkSubmitOperator(
        application=f"{SPARK_HOME}/jobs/submit_pollution_job.py",  # Path accessible by Spark
        task_id="ingest_pollution_job",
        conn_id="spark_default",  # Connection to Spark cluster
        packages='com.datastax.spark:spark-cassandra-connector_2.12:3.4.0',
        verbose=True
    )
    create_cassandra_keyspace_tables >> [ingest_meteo_job, ingest_pollution_job]