from Cassandra.cluster import Cluster

def connect_cassandra():
    try:
        cluster = Cluster(['cassandra'])
        
        cas_session=cluster.connect()
        
        return cas_session
    except Exception as e:
        print(f"Error when Connecting to Cassandra Cluster {e}")
        
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
    except Exception as e:
        print(f"Failed to Create Cassandra meteo Table due to : {e}")
        
def create_cassandra_pollution_table(session):
    try:
        session.execute("""
                        DROP TABLE IF EXISTS metz_meteo.pollution;
                        """)
        session.execute("""
                        CREATE TABLE metz_meteo.meteo (
                            id UUID PRIMARY KEY,
                            lon TEXT,
                            lat TEXT,
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
        
def create_cassandra_keyspace(session):
    try:
        session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS metz_meteo
                        WITH replication = {'class': 'SimpleStrategy','replication_factor':'1'}
                        """)
        print("Cassandra Keyspace created successfully")
    except Exception as e:
        print(f'Could not create cassandra keyspace due to {e}')     
        
if __name__ == "__main__":
    try:
        session=connect_cassandra()
        if session is not None:
            create_cassandra_keyspace(session)
            create_cassandra_meteo_table(session)
            create_cassandra_pollution_table(session)
    except Exception as e :
        print(f"Failed to Connect to Cassandra Cluster/Create Tables Due to {e}")
    