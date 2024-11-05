from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from airflow import DAG
import requests

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
        

def transform_data(res):
    
    try:
        spark_s=SparkSession.builder \
            .appName("WeatherDataSpark") \
            .config("spark.jars","mysql-connector-java-8.0.13.jar") \
            .getOrCreate()

    except Exception as e:
        print(f"Could not create Spark Session due to {e}")

    df=spark_s.createDataFrame(res)
    print(df)

def get_request_weather_data():
    # 49.111459997943115, 6.175108444784084
    url=f"https://api.openweathermap.org/data/2.5/weather?lat=6.175108444784084&lon=49.111459997943115&appid=54ce3be99b6d93efb221eee5b5a8b52a"
    try:
        res=requests.get(url)
        res=res.json()
        transform_data(res)
    except Exception as e:
        print(f"Exception when get request : {e}")

with DAG (
    'api_test_v1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    
    test_api=PythonOperator(
        task_id='test_api',
        python_callable=get_request_weather_data
    )
    
    test_api