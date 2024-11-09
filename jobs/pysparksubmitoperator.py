import requests
from pyspark.sql import SparkSession

def transform_data(res):
    
    try:
        spark_s=SparkSession.builder \
            .appName("WeatherDataSpark") \
            .getOrCreate()
            

        # 49.111459997943115, 6.175108444784084
        url=f"https://api.openweathermap.org/data/2.5/weather?lat=6.175108444784084&lon=49.111459997943115&appid=54ce3be99b6d93efb221eee5b5a8b52a"
        res=requests.get(url)
        res=res.json()
        df=spark_s.createDataFrame(res)
        print(df)
    except Exception as e:
        print(f"Spark Submit Task Error :{e}")


    

