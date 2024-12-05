from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import udf, from_unixtime, to_timestamp, format_number, col
import requests
import os
import uuid


api_key=os.getenv("WEATHER_API_KEY")
schema=StructType([
    StructField("lat", StringType(), True),
    StructField("lon", StringType(), True),
    StructField("aqi", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("no", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("pm2_5", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("nh3", DoubleType(), True),
    StructField("dt", StringType(), True)
])

def create_spark_cassandra_connection():   
    try:
        s_conn=SparkSession.builder \
            .appName("Pollution_Spark_Cassandra_Connection") \
            .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
            .config('spark.cassandra.connection.host','cassandra') \
            .getOrCreate()
        # s_conn.conf.set("spark.cassandra.input.schema.forceGetMetadata", "true")

        return s_conn
    except Exception as e:
        print(f'Cassandra Spark Connection Error: {e}')

def get_air_pollution_data():
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
    return flatten_obj


def air_pollution_trans(data):
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
      elif field not in ['id','dt']:
        df=df.withColumn(field, format_number(col(field),2))

    return df

if __name__=="__main__":
    spark_cassandra_session=create_spark_cassandra_connection()
    
    if spark_cassandra_session is not None:
        data=get_air_pollution_data()
        df=air_pollution_trans(data)
        try:
            df.write.format("org.apache.spark.sql.cassandra") \
            .options(table="pollution",keyspace="metz_meteo") \
            .mode("append") \
            .save()
            print("[Pollution] Data Written successfuly !")

        except Exception as e:
            print(f"[Pollution] Failed to write data to cassandra due to : {e}")
