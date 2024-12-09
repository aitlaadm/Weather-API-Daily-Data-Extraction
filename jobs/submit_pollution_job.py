from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import udf, from_unixtime, to_timestamp, format_number, col
from airflow.exceptions import AirflowException
from cassandra.cluster import Cluster
import requests
import os
import uuid


api_key=os.getenv("WEATHER_API_KEY")
schema=StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("aqi", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("no", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("nh3", DoubleType(), True),
    StructField("dt", IntegerType(), True)
])
def connect_cassandra():
    try:
        cluster = Cluster(['cassandra'])
        
        cas_session=cluster.connect()
        
        return cas_session
    except AirflowException as e:
        print(f"Error when Connecting to Cassandra Cluster {e}")
        
def create_cassandra_keyspace(session):
    try:
        session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS metz_meteo
                        WITH replication = {'class': 'SimpleStrategy','replication_factor':'1'}
                        """)
        print("Cassandra Keyspace created successfully")
    except AirflowException as e:
        print(f'Could not create cassandra keyspace due to {e}') 
        
def create_cassandra_pollution_table(session):
    try:
        session.execute("""
                        DROP TABLE IF EXISTS metz_meteo.pollution;
                        """)
        session.execute("""
                        CREATE TABLE metz_meteo.pollution (
                            id UUID PRIMARY KEY,
                            lon FLOAT,
                            lat FLOAT,
                            aqi FLOAT,
                            co FLOAT,
                            no FLOAT,
                            no2 FLOAT,
                            o3 FLOAT,
                            so2 FLOAT,
                            pm2_5 FLOAT,
                            pm10 FLOAT,
                            nh3 FLOAT,
                            dt TIMESTAMP
                            );
                        """)
        print("Cassandra table Created Successfully !")
    except Exception as e:
        print(f"Failed to Create Cassandra pollution Table due to : {e}")
        
def create_spark_cassandra_connection():   
    try:
        s_conn=SparkSession.builder \
            .appName("Pollution_Spark_Cassandra_Connection") \
            .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config('spark.cassandra.connection.host','cassandra') \
            .getOrCreate()
        # s_conn.conf.set("spark.cassandra.input.schema.forceGetMetadata", "true")

        return s_conn
    except AirflowException as e:
        print(f'Cassandra Spark Connection Error: {e}')

def get_air_pollution_data():
  try:
    url=f"https://api.openweathermap.org/data/2.5/air_pollution?lat=49.111459997943115&lon=6.175108444784084&lang=fr&appid={api_key}"
    res=requests.get(url)

    res_obj=res.json()
    flatten_obj={}

    for at, val in res_obj.items():
      if isinstance(val, list) and len(val)==1 and isinstance(val[0],dict):
        for i,j in val[0].items():
          if isinstance(j, dict):
            flatten_obj.update(j)
          else:
            flatten_obj[i]=j
      elif isinstance(val, dict):
        flatten_obj.update(val)
      else:
        flatten_obj[at]=val 
    # Convert all integers to float except dt
    for n,v in flatten_obj.items():
      if n != 'dt' and isinstance(v,int):
        flatten_obj[n]=float(v)
    return flatten_obj
  except AirflowException as e:
    print(f"[Pollution] Failed to Get/Flatten Data {e}")

def air_pollution_trans(data):
  try:
    spark_s=SparkSession.builder \
        .appName("AirPollutionApp") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    df=spark_s.createDataFrame([data], schema=schema)
    #Add uuid id field
    uuidudf=udf(lambda: str(uuid.uuid4()), StringType())
    df=df.withColumn("id", uuidudf())
    #tansformations: turn unix timestamp to normal timestamp, format decimal numbers to 2 numbers after comma
    for field in df.columns:
      if field=="dt":
        df=df.withColumn(field, to_timestamp(from_unixtime(col(field)),"yyyy-MM-dd HH:mm:ss"))
      elif field not in ['id','dt','lat','lon','aqi','nh3']:
        df=df.withColumn(field, format_number(col(field),2))

    return df
  except AirflowException as e:
    print(f"[Pollution] Failed to Transform Data Due to : {e}")
    
if __name__=="__main__":
    spark_cassandra_session=create_spark_cassandra_connection()
    
    if spark_cassandra_session is not None:
        session=connect_cassandra()
        if session is not None:
          create_cassandra_keyspace(session)
          create_cassandra_pollution_table(session)
          data=get_air_pollution_data()
          df=air_pollution_trans(data)
          try:
              df.write.format("org.apache.spark.sql.cassandra") \
              .options(table="pollution",keyspace="metz_meteo") \
              .mode("append") \
              .save()
              session.shutdown()
              spark_cassandra_session.stop()
              print("[Pollution] Data Written successfuly !")

          except AirflowException as e:
              print(f"[Pollution] Failed to write data to cassandra due to : {e}")
