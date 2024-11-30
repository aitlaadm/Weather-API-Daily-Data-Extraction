import requests
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, format_number, from_unixtime
# from airflow.models.xcom import XCom

schema=StructType(
  [    StructField("lon", DoubleType(), True),
      StructField("lat", DoubleType(), True),
      StructField("id", StringType(), True),
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
      StructField("sunrise", IntegerType(), True),
      StructField("sunset", IntegerType(), True),
      StructField("timezone", IntegerType(), True),
      StructField("name", StringType(), True),
      StructField("cod", IntegerType(), True)]
  )
def connect_cassandra():
    try:
        cluster = Cluster(['localhost'])
        
        cas_session=cluster.connect()
        
        return cas_session
    except Exception as e:
        print(f"Error when Connecting to Cassandra Cluster {e}")
def create_cassandra_table(session):
    session.execute("""
                    CREATE TABLE IF NOT EXISTS metz_meteo.meteo (
                        id PRIMARY KEY,
                        lon TEXT,
                        lat TEXT,
                        main TEXT
                        description TEXT,
                        icon TEXT,
                        base TEXT,
                        temp DOUBLE,
                        feels_like DOUBLE,
                        temp_max DOUBLE,
                        temp_min DOUBLE,
                        pressure TEXT,
                        humidity INT,
                        sea_level INT,
                        visibility INT,
                        speed DOUBLE,
                        deg TEXT,
                        all INT,
                        df DATE,
                        country TEXT,
                        sunrise DATE,
                        sunset DATE,
                        timezone INT,
                        name TEXT,
                        cod INT
                        );
                    """)
def create_spark_cassandra_connection():
    try:
        s_conn=SparkSession.builder \
            .appName("Spark_Cassandra_Connection") \
            .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.13:3.4.1") \
            .config('spark.cassandra.connection.host','localhost') \
            .getOrCreate()
        return s_conn
    except Exception as e:
        print(f'Cassandra Spark Connection Error: {e}')
        
def create_cassandra_keyspace(session):
    try:
        session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS metz_meteo
                        WITH replication = {'class': 'SimpleStrategy','replication_factor':'1'}
                        """)
        print("Cassandra Keyspace created successfully")
    except Exception as e:
        print(f'Could not create cassandrakeyspace due to {e}')
        
def get_clean_data():
    
  try:
      spark_s=SparkSession.builder \
          .appName("WeatherDataSpark") \
          .getOrCreate()
      sc=spark_s.sparkContext
      # Metz coordinates: 49.111459997943115, 6.175108444784084
      url=f"https://api.openweathermap.org/data/2.5/weather?lat=49.111459997943115&lon=6.175108444784084&lang=fr&appid=54ce3be99b6d93efb221eee5b5a8b52a"
      res=requests.get(url)
      obj=res.json()
      #create df from flatten / suitable format nested json objects
      flatten_obj={}
      for at, v in obj.items():
        if isinstance(v, dict):  # Vérifie si c'est un dictionnaire
            flatten_obj.update(v)
        elif isinstance(v, list) and len(v) == 1 and isinstance(v[0], dict):  
            # Vérifie si c'est une liste avec un seul dictionnaire
            flatten_obj.update(v[0])
        else:
            flatten_obj[at] = v
          
      df=spark_s.createDataFrame(data=[flatten_obj],schema=schema)
      return df
  except Exception as e:
      print(f"Spark Submit Task Error :{e}")
      
def data_trans_clean():
  df=get_clean_data()
  # Convert temperatures from kalvin to celsuis
  temp_cols=['temp','feels_like','temp_min','temp_max']
  for field in temp_cols:
    df=df.withColumn(field, format_number(col(field)-273.15,2))
#Convert from unix time to datetime
  time_cols=['dt','sunrise','sunset']
  #convert time values from unix time to UTC datetime & persist df in cache
  for field in time_cols:
    df=df.withColumn(field, from_unixtime(col(field)))
    return df
    
if __name__=="__main__":
    s_c=create_spark_cassandra_connection()
    
    if s_c is not None:
        session = connect_cassandra()
        df=data_trans_clean()
        if session is not None:
            create_cassandra_keyspace(session)
            create_cassandra_table(session)
            
            insert_query=df.writeStream.format("org.apache.spark.sql.cassandra") \
                            .option('checkpointLocation', 'tmp/checkpoint') \
                            .option('keyspace','metz_meteo') \
                            .option('table', 'meteo') \
                            .start()
            insert_query.awaitTermination()