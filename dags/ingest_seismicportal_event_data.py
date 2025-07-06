# Data API details: https://www.seismicportal.eu/fdsn-wsevent.html

import requests
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_seismicportal_event_data',
    default_args=default_args,
    description='Fetch seismic event data and ingest into Postgres',
    schedule_interval='@daily',
    start_date=datetime(2025, 7, 1),
    catchup=True,
    concurrency=1,
    tags=['seismicportal.eu', 'fdsn-wsevent'],
    params={
        'lat': -42,  # Default latitude for the query
        'lon': 174,  # Default longitude for the query
        'minradius': 0,  # Default minimum radius for the query
        'maxradius': 10,  # Default maximum radius for the query
    }
)

API_URL_TEMPLATE = "https://www.seismicportal.eu/fdsnws/event/1/query?&format=json&lat={lat}&lon={lon}&minradius={minradius}&maxradius={maxradius}&start={start}&end={end}"

def fetch_seismic_data(start: str, end: str, lat, lon, minradius, maxradius):
    api_url = API_URL_TEMPLATE.format(
        start=start,
        end=end,
        lat=lat,
        lon=lon,
        minradius=minradius,
        maxradius=maxradius
    )
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    # The events are usually under the 'features' key in GeoJSON
    events = data.get('features', [])
    if not events:
        return pd.DataFrame()
    # Flatten the GeoJSON structure
    df = pd.json_normalize(events)
    # Add a column for the raw GeoJSON event
    df['raw_event'] = [json.dumps(event) for event in events]
    return df

def ingest_to_postgres(df: pd.DataFrame, table_name: str = "flat_seismic_events", schema: str = "raw"):
    if df.empty:
        print("No data to ingest.")
        return
    # Use Airflow's PostgresHook
    hook = PostgresHook(postgres_conn_id="db_conn")
    engine = hook.get_sqlalchemy_engine()
    # Ingest data into the specified schema and table, append if table exists
    df.to_sql(table_name, engine, if_exists="append", index=False, schema=schema)
    print(f"Ingested {len(df)} rows into table '{schema}.{table_name}'.")

def fetch_and_ingest_seismic_data(**context):
    # Get Airflow's data_interval_start and data_interval_end as UTC ISO strings
    start = context['data_interval_start'].strftime('%Y-%m-%dT%H:%M:%S')
    end = context['data_interval_end'].strftime('%Y-%m-%dT%H:%M:%S')
    params = context['params']
    df = fetch_seismic_data(start, end, params['lat'], params['lon'], params['minradius'], params['maxradius'])
    ingest_to_postgres(df)

fetch_and_ingest_task = PythonOperator(
    task_id='fetch_and_ingest_seismic_data',
    python_callable=fetch_and_ingest_seismic_data,
    dag=dag,
)