from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from airflow.exceptions import AirflowException
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

# meteo_data=[]
# pollution_data=[]

def connect_cassandra():
    try:
        cluster = Cluster(['cassandra'])
        
        cas_session=cluster.connect()
        
        return cas_session
    except AirflowException as e:
        print(f"Error when Connecting to Cassandra Cluster {e}")
        
def create_final_cassandra_table(session):
    try:
        session.execute("""
                        CREATE TABLE IF NOT EXISTS metz_meteo.meteo_pollution_data (
                                id UUID PRIMARY KEY,
                                lon TEXT,
                                lat TEXT,
                                main TEXT,
                                description TEXT,
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
                                dt_meteo TIMESTAMP,
                                country TEXT,
                                sunrise TIMESTAMP,
                                sunset TIMESTAMP,
                                timezone INT,
                                name TEXT,
                                aqi FLOAT,
                                co FLOAT,
                                no FLOAT,
                                no2 FLOAT,
                                o3 FLOAT,
                                so2 FLOAT,
                                pm2_5 FLOAT,
                                pm10 FLOAT,
                                nh3 FLOAT,
                                dt_pollution TIMESTAMP
                        )
                        """)
        print("Meteo_pollution_data table Created !")
    except AirflowException as e:
        print(f"Failed to create meteo_pollution_data table due to {e}")
        
def create_get_meteo_data_session():
    try:
        spark_s=SparkSession.builder \
                .appName("Read_Meteo_Data_App") \
                .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
                .config('spark.cassandra.connection.host','cassandra') \
                .getOrCreate()
        return spark_s
    except AirflowException as e:
        print(f"Failed to extract meteo data due to {e}")
        
def get_meteo_data(spark_session):
    data=spark_session.read.format("org.apache.spark.sql.cassandra") \
        .options(table="meteo", keyspace="metz_meteo") \
        .load()
    return data     
    
def get_pollution_data(spark_session):
    data=spark_session.read.format("org.apache.spark.sql.cassandra") \
        .options(table="pollution", keyspace="metz_meteo") \
        .load()
    return data     

def write_final_data(meteo_df, pollution_df):
    try:
        #cleaning data
        pollution_df=pollution_df.drop("id")
        meteo_df=meteo_df.drop("all","id_meteo","icon","id","cod")
        
        meteo_df=meteo_df.withColumnRenamed("dt","dt_meteo")
        pollution_df=pollution_df.withColumnRenamed("dt","dt_pollution")
        
        df_join=meteo_df.join(pollution_df, on=['lat','lon'])
        
        
        udfuuid=udf(lambda: str(uuid.uuid4()), StringType())
        
        df_union=df_join.withColumn("id", udfuuid())
        
        print(df_join.show())
        df_union.write.format("org.apache.spark.sql.cassandra") \
            .options(table="meteo_pollution_data", keyspace="metz_meteo") \
            .mode("append") \
            .save()
            
    except AirflowException as e:
        print(f"Failed to write final data due to {e}")
    
if __name__ == "__main__":
    try:
        spark_session=create_get_meteo_data_session()
        if spark_session is not None:
            session=connect_cassandra()
            if session is not None:
                create_final_cassandra_table(session)
                meteo_data=get_meteo_data(spark_session)
                pollution_data=get_pollution_data(spark_session)

                # df_union=meteo_data.unionByName(pollution_data, allowMissingColumns=True)
                write_final_data(meteo_data, pollution_data)
                session.shutdown()
                spark_session.stop()
            print("Pollution & Meteo Data Combined Written Successfully !")
    except AirflowException as e :
        print(f"Failed to Create Spark Session or to Connect to Cassandra Cluster Due to {e}")
    