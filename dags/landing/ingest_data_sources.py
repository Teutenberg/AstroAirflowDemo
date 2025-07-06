"""
Airflow DAG to ingest data from a URI (file, database, or API), auto-detect source type, infer schema, and load into Postgres.
"""
import os
import json
import pandas as pd
import requests
from sqlalchemy import create_engine, inspect
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, date
import yaml
from jinja2 import Template
import re
import pendulum

# Default args for the DAG
DEFAULT_ARGS = {
    'owner': 'Teutenberg',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def detect_source_type(uri):
    """Detects the type of data source based on the URI."""
    if uri.startswith('http://') or uri.startswith('https://'):
        return 'api'
    if uri.startswith('postgresql://') or uri.startswith('mysql://') or uri.startswith('sqlite://'):
        return 'database'
    if os.path.isfile(uri):
        ext = os.path.splitext(uri)[1].lower()
        if ext in ['.csv', '.tsv']:
            return 'csv'
        if ext in ['.json']:
            return 'json'
        if ext in ['.parquet']:
            return 'parquet'
    raise ValueError(f"Could not detect source type for URI: {uri}")

def load_data(uri, source_type, params=None):
    """Loads data from the source and returns a pandas DataFrame."""
    if source_type == 'csv':
        return pd.read_csv(uri)
    if source_type == 'json':
        return pd.read_json(uri)
    if source_type == 'parquet':
        return pd.read_parquet(uri)
    
    # If it's an API, we expect a JSON response
    if source_type == 'api':
        # Pass params as query string if provided
        if params:
            resp = requests.get(uri, params=params)
        else:
            resp = requests.get(uri)
        resp.raise_for_status()
        data = resp.json()
        # Try to normalize if it's a list or dict
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    return pd.json_normalize(v)
            return pd.json_normalize(data)
        raise ValueError("API did not return JSON list or dict")
    
    # If it's a database, we expect a table name in the URI query string
    if source_type == 'database':
        # For demo: expects a table name in the URI query string, e.g. postgresql://.../db?table=tablename
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(uri)
        qs = parse_qs(parsed.query)
        table = qs.get('table', [None])[0]
        if not table:
            raise ValueError("Database URI must include ?table=tablename")
        engine = create_engine(uri.split('?')[0])
        return pd.read_sql_table(table, engine)
    raise ValueError(f"Unsupported source type: {source_type}")

def ingest_data(df, table_name, schema_name, hook):
    """Ingests the DataFrame into the database table."""
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, if_exists='append', index=False, schema=schema_name)

def render_yaml_config(yaml_path, context):
    """Read and render a YAML file as a Jinja template using Airflow context."""
    with open(yaml_path, "r") as f:
        raw = f.read()
    rendered = Template(raw).render(**context)
    return yaml.safe_load(rendered)

def create_ingest_dag(source_config):
    dag_id = f"ingest_{source_config['name'].replace('.', '_')}"
    default_args = DEFAULT_ARGS.copy()
    description = source_config.get('description', '')
    uri = source_config.get('uri', '')
    params = source_config.get('params', {})

    # Extract dag_configs from source_config, if present
    dag_configs = source_config.get('dag_configs', {})
    schedule_interval = dag_configs.get('schedule', None)
    start_date = dag_configs.get('start_date', '2025-07-01')
    if isinstance(start_date, str):
        start_date = pendulum.parse(start_date)
    elif isinstance(start_date, datetime):
        start_date = pendulum.instance(start_date)
    elif isinstance(start_date, date):
        start_date = pendulum.datetime(start_date.year, start_date.month, start_date.day)
    catchup = dag_configs.get('catchup', False)
    concurrency = dag_configs.get('concurrency', None)
    tags = dag_configs.get('tags', [])

    def _ingest_data_source(**context):
        """Task to ingest data from the configured source into Postgres."""
        table_name = source_config['name']
        print(f"[DAG: {dag_id}] Ingesting from URI: {uri} with params: {params}")
        source_type = detect_source_type(uri)
        print(f"Detected source type: {source_type}")

        api_params = dict(params) if source_type == 'api' else None
        df = load_data(uri, source_type, params=api_params)
        print(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

        # Use conn_id and schema from dag params
        conn_id = context['dag'].params['conn_id']
        schema = context['dag'].params['schema']
        hook = PostgresHook(postgres_conn_id=conn_id)
        ingest_data(df, table_name, schema, hook)
        print(f"Ingested {len(df)} rows into {schema}.{table_name}")

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=catchup,
        tags=tags,
        concurrency=concurrency,
        params={
            "conn_id": dag_configs.get("conn_id", "db_conn"),
            "schema": dag_configs.get("schema", "raw"),
        },
    )

    PythonOperator(
        task_id='ingest_data_sources',
        python_callable=_ingest_data_source,
        dag=dag,
    )
    return dag

# Render the YAML config at module load to create DAGs dynamically
# Use absolute path to avoid double 'landing/landing' bug
config_path = os.path.join(os.path.dirname(__file__), "ingest_data_sources.yml")
config = render_yaml_config(config_path, {'data_interval_start': datetime(2025, 7, 1), 'data_interval_end': datetime(2025, 7, 2)})
for source in config.get('data_sources', []):
    dag_name = re.sub(r'\W', '_', source['name'])
    globals()[f"ingest_{dag_name}_dag"] = create_ingest_dag(source)
