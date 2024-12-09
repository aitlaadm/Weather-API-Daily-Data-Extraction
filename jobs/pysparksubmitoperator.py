import requests
import uuid
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, format_number, from_unixtime, udf, to_timestamp
from airflow.exceptions import AirflowException
import os

# load variables from .env
api_key=os.getenv("WEATHER_API_KEY")
schema=StructType(
  [    StructField("lon", DoubleType(), True),
      StructField("lat", DoubleType(), True),
      StructField("id_meteo", StringType(), True),
      StructField("main", StringType(), True),
      StructField("description", StringType(), True),
      StructField("icon", StringType(), True),
      StructField("base", StringType(), True),
      StructField("temp", DoubleType(), True),
      StructField("feels_like", DoubleType(), True),
      StructField("temp_min", DoubleType(), True),
      StructField("temp_max", DoubleType(), True),
      StructField("pressure", StringType(), True),
      StructField("humidity", IntegerType(), True),
      StructField("sea_level", IntegerType(), True),
      StructField("grnd_level", IntegerType(), True),
      StructField("visibility", IntegerType(), True),
      StructField("speed", DoubleType(), True),
      StructField("deg", StringType(), True),
      StructField("all", IntegerType(), True),
      StructField("dt", StringType(), True),
      StructField("country", StringType(), True),
      StructField("sunrise", StringType(), True),
      StructField("sunset", StringType(), True),
      StructField("timezone", IntegerType(), True),
      StructField("name", StringType(), True),
      StructField("cod", IntegerType(), True)]
  )
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
        
def create_cassandra_meteo_table(session):
    try:
        session.execute("""
                        DROP TABLE IF EXISTS metz_meteo.meteo;
                        """)
        session.execute("""
                        CREATE TABLE metz_meteo.meteo (
                            id UUID PRIMARY KEY,
                            id_meteo TEXT,
                            lon TEXT,
                            lat TEXT,
                            main TEXT,
                            description TEXT,
                            icon TEXT,
                            base TEXT,
                            temp FLOAT,
                            feels_like FLOAT,
                            temp_max FLOAT,
                            temp_min FLOAT,
                            pressure TEXT,
                            humidity INT,
                            sea_level FLOAT,
                            grnd_level FLOAT,
                            visibility INT,
                            speed FLOAT,
                            deg TEXT,
                            all INT,
                            dt TIMESTAMP,
                            country TEXT,
                            sunrise TIMESTAMP,
                            sunset TIMESTAMP,
                            timezone INT,
                            name TEXT,
                            cod INT
                            );
                        """)
        print("Cassandra table Created Successfully !")
    except AirflowException as e:
        print(f"Failed to Create Cassandra meteo Table due to : {e}")
        
def create_spark_cassandra_connection():   
    try:
        s_conn=SparkSession.builder \
            .appName("Meteo_Spark_Cassandra_Connection") \
            .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config('spark.cassandra.connection.host','cassandra') \
            .getOrCreate()

        return s_conn
    except AirflowException as e:
        print(f'Cassandra Spark Connection Error: {e}')
        
        
def get_clean_data():
  try:
    spark_s=SparkSession.builder \
        .appName("WeatherDataApp") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    # Metz coordinates: 49.111459997943115, 6.175108444784084
    url=f"https://api.openweathermap.org/data/2.5/weather?lat=49.111459997943115&lon=6.175108444784084&lang=fr&appid={api_key}"
    res=requests.get(url)
    obj=res.json()
    
    #create df from flatten / suitable format nested json objects
    flatten_obj={}
    for at, v in obj.items():
        if isinstance(v, dict): 
            flatten_obj.update(v)
        elif isinstance(v, list) and len(v) == 1 and isinstance(v[0], dict):  
            # Vérifie si c'est une liste avec un seul dictionnaire
            flatten_obj.update(v[0])
        else:
            flatten_obj[at] = v
            
    df=spark_s.createDataFrame([flatten_obj],schema=schema)
    uuidudf=udf(lambda: str(uuid.uuid4()),StringType())
    df_with_uuid=df.withColumn('id',uuidudf())
    
    return df_with_uuid
  except AirflowException as e:
      print(f"[Meteo] Failed to Get/Flatten Data {e}")
      
def data_trans_clean():
  try:
    df=get_clean_data()
    # Convert temperatures from kalvin to celsuis
    temp_cols=['temp','feels_like','temp_min','temp_max']
    for field in temp_cols:
      df=df.withColumn(field, format_number(col(field)-273.15,2))
    #Convert from unix time to datetime
    time_cols=['dt','sunrise','sunset']
    #convert time values from unix time to UTC datetime
    for field in time_cols:
      df=df.withColumn(field, to_timestamp(from_unixtime(col(field)),"yyyy-MM-dd HH:mm:ss"))
    return df
  except AirflowException as e:
    print(f"[Pollution] Failed to Transform Data Due to : {e}")
    
if __name__=="__main__":
    s_c=create_spark_cassandra_connection()
    if s_c is not None:
      session=connect_cassandra()
      if session is not None:
        create_cassandra_keyspace(session)
        create_cassandra_meteo_table(session)
        df=data_trans_clean()
        try:
            df.write.format("org.apache.spark.sql.cassandra") \
            .options(table='meteo',keyspace='metz_meteo') \
            .mode("append") \
            .save()
            session.shutdown()
            s_c.stop()
            print("[Meteo] Data Written successfuly !")
        except AirflowException as e:
            print(f"[Meteo] Failed to write data to cassandra due to : {e}")
