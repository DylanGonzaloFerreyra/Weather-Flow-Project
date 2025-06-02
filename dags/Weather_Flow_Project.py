import requests
import json  
import pandas as pd 
from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, avg, min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import supabase  
from airflow import DAG 
from airflow.operators.python import PythonOperator  
from datetime import datetime, timedelta
# definir mis variables 
API_KEY = ""
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
CITIES = ["Buenos Aires", "Madrid", "Tokyo", "New York", "London", "Brasilia"]
supabase_url = ""
supabase_key = ""
default_args = {
    "owner": "Dylan",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 8),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "Weather_Flow_Project",
    default_args=default_args,
    schedule_interval="0 6 * * *",
) #que inicie tipo 6 de la mañamna

def get_data_from_api():
    data = []
    for q in CITIES:
        params = {
            "q": q,
            "appid": API_KEY,
            "units": "metric"
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            json_data = response.json()
            city_data = {
                "city": json_data["name"],
                "temperature": json_data["main"]["temp"],
                "humidity": json_data["main"]["humidity"],
                "pressure": json_data["main"]["pressure"],
            }
            data.append(city_data)
            print(f"Data fetched correctly for {q}: {city_data}")
        else:
            print(f"Error fetching data for {q}: {response.status_code}")
    return data

get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_data_from_api,
    dag=dag,
)

def process_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_data')
    if not data:
        print("No data received from the API.")

    for entry in data:
        entry["temperature"] = float(entry["temperature"])
        entry["humidity"] = float(entry["humidity"])
        entry["pressure"] = float(entry["pressure"])
    #For some reason if i dont write this, all explodes

    spark = SparkSession.builder.appName("DataWeatherProcessing").getOrCreate()
    df = spark.createDataFrame(data)    
    raw_data = [row.asDict() for row in df.collect()]
    df_summary = df.groupBy().agg(
        avg("temperature").alias("avg_temp"),
        min("temperature").alias("min_temp"),
        max("temperature").alias("max_temp"),
        avg("humidity").alias("avg_humidity"),
        avg("pressure").alias("avg_pressure")
    )
    df_summary.show()
    result = [row.asDict() for row in df_summary.collect()]
    return {"raw_data": raw_data, "calculated_data": result}

process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag,
)

def save_to_supabase(**kwargs):   
    ti= kwargs['ti']
    data = ti.xcom_pull(task_ids='process_data')
    if not data:
        print("No data received from the API.")
        return {"raw_data": [], "calculated_data": []}  # Retorna un diccionario vacío en caso de error
    raw_data = data.get("raw_data", [])
    calculated_data = data.get("calculated_data", [])
    if not raw_data and not calculated_data:
        print("No data to save to Supabase.")
        return
    supabase_client = supabase.create_client(supabase_url, supabase_key)
    for row in raw_data:
        supabase_client.table("city_weather").insert(row).execute()
    print("Data saved to Supabase correctly.")
    for row in calculated_data:
        row["city"] = "Global"
        supabase_client.table("weather_summary").insert(row).execute()
    print("The calculated data have saved to Supabase correctly.")        
save_data = PythonOperator(
    task_id="save_data",
    python_callable=save_to_supabase,
    dag=dag,
)
get_data >> process_data_task >> save_data