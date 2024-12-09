from datetime import datetime, timedelta
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
import os

SPARK_HOME=os.getenv("SPARK_HOME")

default_args={
    'owner': "Simo",
    'retries':3,
    'start_date': datetime(2024,12,5),
    'retry_delay': timedelta(minutes=1)
}


with DAG (
    'api_test_v2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

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
    
    get_data_from_db_job = SparkSubmitOperator(
        application=f"{SPARK_HOME}/jobs/read_pollution_meteo_data.py",  # Path accessible by Spark
        task_id="get_data_from_db_job",
        conn_id="spark_default",  # Connection to Spark cluster
        packages='com.datastax.spark:spark-cassandra-connector_2.12:3.4.0',
        trigger_rule='all_success',
        verbose=True
    )
    ingest_meteo_job >> ingest_pollution_job >> get_data_from_db_job