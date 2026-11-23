from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pendulum
import requests
import json

API_CONN_ID = "open_meteo"

with DAG(
    "weather_data",
    default_args={"owner":"airflow", "retries": 1,"retry_delay": pendulum.duration(minutes=1)},
    start_date=pendulum.datetime(2025, 4, 22, tz="UTC"),
    schedule="@daily",
    catchup = False,
    tags=["weather", "data", "pipeline"],
    max_active_runs=1
  
) as dag:

    @task
    def extract_weather():
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method="GET")
        params={
             "latitude": 35.6895,
                "longitude": 139.6917,
                "current": (
                  "precipitation,"
                  "weather_code,"
                  "temperature_2m,"
                  "relative_humidity_2m,"
                  "apparent_temperature,"
                  "is_day,"
                  "pressure_msl"
                ),
                "timezone": "GMT"
        }
        response=http_hook.run(endpoint="/v1/forecast",data=params,extra_options={"timeout": 30})
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data. Status code: {response.status_code}")
    
    @task 
    def transform_weather(weather_data):
        current= weather_data["current"]
        transformed_data=[]
        gmt_timestamp = pendulum.parse(current["time"],tz="UTC")
        transformed_data.append((
                    gmt_timestamp,
                    current["temperature_2m"],
                    current["relative_humidity_2m"],
                    current["precipitation"],
                    current["pressure_msl"],
                    current["weather_code"],
                    current["apparent_temperature"],
                    bool(current["is_day"])
                    
            ))
        return transformed_data
        
    @task
    def load_weather(transformed_data):
            pg_hook = PostgresHook(postgres_conn_id="postgres_default")
            conn=pg_hook.get_conn()
            cursor=conn.cursor()
            create_sql="""CREATE TABLE IF NOT EXISTS\
                           WEATHER_DATA(OBSERVATION_TIME TIMESTAMPTZ PRIMARY KEY,TEMPERATURE DOUBLE PRECISION,\
                           HUMIDITY DOUBLE PRECISION,PRECIPITATION DOUBLE PRECISION,PRESSURE DOUBLE PRECISION,WEATHER_CODE INTEGER,\
                           APPARENT_TEMPERATURE DOUBLE PRECISION,IS_DAY BOOLEAN\
                           )"""
            insert_sql = """
                            INSERT INTO weather_data (
                            OBSERVATION_TIME,
                            TEMPERATURE,
                            HUMIDITY,
                            PRECIPITATION,
                            PRESSURE,
                            WEATHER_CODE,
                            APPARENT_TEMPERATURE,
                            IS_DAY
                        )
                        VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (OBSERVATION_TIME)
                        DO UPDATE SET
                            TEMPERATURE = EXCLUDED.TEMPERATURE,
                            HUMIDITY = EXCLUDED.HUMIDITY,
                            PRECIPITATION = EXCLUDED.PRECIPITATION,
                            PRESSURE = EXCLUDED.PRESSURE,
                            WEATHER_CODE = EXCLUDED.WEATHER_CODE,
                            APPARENT_TEMPERATURE = EXCLUDED.APPARENT_TEMPERATURE,
                            IS_DAY = EXCLUDED.IS_DAY;
                    """
            cursor.execute(create_sql)
            cursor.executemany(insert_sql,transformed_data)
            conn.commit()
            conn.close()
            return "Data loaded successfully into the database."



    weather_data=extract_weather()
    transformed_data=transform_weather(weather_data)
    load_weather(transformed_data)