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
from airflow.utils.log.logging_mixin import LoggingMixin

# Default args for the DAG
DEFAULT_ARGS = {
    'owner': 'Teutenberg',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

logger = LoggingMixin().log

def detect_source_type(uri):
    """Detects the type of data source based on the URI."""
    logger.info(f"Detecting source type for URI: {uri}")
    if uri.startswith('http://') or uri.startswith('https://'):
        logger.info("Source type detected as 'api'.")
        return 'api'
    if uri.startswith('postgresql://') or uri.startswith('mysql://') or uri.startswith('sqlite://'):
        logger.info("Source type detected as 'database'.")
        return 'database'
    if os.path.isfile(uri):
        ext = os.path.splitext(uri)[1].lower()
        logger.info(f"File extension detected: {ext}")
        if ext in ['.csv', '.tsv']:
            logger.info("Source type detected as 'csv'.")
            return 'csv'
        if ext in ['.json']:
            logger.info("Source type detected as 'json'.")
            return 'json'
        if ext in ['.parquet']:
            logger.info("Source type detected as 'parquet'.")
            return 'parquet'
    logger.error(f"Could not detect source type for URI: {uri}")
    raise ValueError(f"Could not detect source type for URI: {uri}")

def load_data(uri, source_type, params=None):
    """Loads data from the source and returns a pandas DataFrame."""
    logger.info(f"Loading data from URI: {uri} as source_type: {source_type}")
    if source_type == 'csv':
        logger.info("Reading CSV file.")
        return pd.read_csv(uri)
    if source_type == 'json':
        logger.info("Reading JSON file.")
        return pd.read_json(uri)
    if source_type == 'parquet':
        logger.info("Reading Parquet file.")
        return pd.read_parquet(uri)
    
    # If it's an API, we expect a JSON response
    if source_type == 'api':
        logger.info(f"Making API request to {uri} with params: {params}")
        # Pass params as query string if provided
        if params:
            resp = requests.get(uri, params=params)
        else:
            resp = requests.get(uri)
        logger.info(f"API response status code: {resp.status_code}")
        resp.raise_for_status()
        data = resp.json()
        logger.info(f"API response JSON type: {type(data)}")
        # Try to normalize if it's a list or dict
        if isinstance(data, list):
            logger.info("API returned a list. Converting to DataFrame.")
            return pd.DataFrame(data)
        if isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    logger.info("API returned a dict with a list value. Normalizing.")
                    return pd.json_normalize(v)
            logger.info("API returned a dict. Normalizing.")
            return pd.json_normalize(data)
        logger.error("API did not return JSON list or dict")
        raise ValueError("API did not return JSON list or dict")
    
    # If it's a database, we expect a table name in the URI query string
    if source_type == 'database':
        logger.info(f"Connecting to database with URI: {uri}")
        # For demo: expects a table name in the URI query string, e.g. postgresql://.../db?table=tablename
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(uri)
        qs = parse_qs(parsed.query)
        table = qs.get('table', [None])[0]
        if not table:
            logger.error("Database URI must include ?table=tablename")
            raise ValueError("Database URI must include ?table=tablename")
        engine = create_engine(uri.split('?')[0])
        logger.info(f"Reading table '{table}' from database.")
        return pd.read_sql_table(table, engine)
    logger.error(f"Unsupported source type: {source_type}")
    raise ValueError(f"Unsupported source type: {source_type}")

def map_dtype_to_sql(dtype):
    """Maps pandas dtypes to SQL types for schema evolution."""
    if pd.api.types.is_integer_dtype(dtype):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE PRECISION'
    if pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    return 'TEXT'

def ingest_data(df, table_name, schema_name, hook):
    """Ingests the DataFrame into the database table, evolving schema as needed."""
    logger.info(f"Preparing to ingest data into {schema_name}.{table_name}")
    # Convert any dict columns to JSON strings
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            logger.info(f"Converting column '{col}' dicts to JSON strings.")
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    # Add a 'raw_data' column with the full row as JSON for schema evolution
    logger.info("Adding 'raw_data' column with row JSON.")
    df['raw_data'] = df.apply(lambda row: row.to_json(), axis=1)
    engine = hook.get_sqlalchemy_engine()
    # Use context manager for connection to ensure closure
    with engine.connect() as conn:
        # --- Schema evolution logic ---
        inspector = inspect(engine)
        table_exists = engine.dialect.has_table(conn, table_name, schema=schema_name)
        if table_exists:
            columns = {col['name'] for col in inspector.get_columns(table_name, schema=schema_name)}
            missing_cols = [col for col in df.columns if col not in columns]
            for col in missing_cols:
                sql_type = map_dtype_to_sql(df[col].dtype)
                logger.info(f"Altering table to add column: {col} {sql_type}")
                alter_sql = f'ALTER TABLE "{schema_name}"."{table_name}" ADD COLUMN "{col}" {sql_type}';
                try:
                    conn.execute(alter_sql)
                except Exception as e:
                    logger.warning(f"Could not add column {col}: {e}")
    logger.info(f"Writing {len(df)} rows to {schema_name}.{table_name}.")
    df.to_sql(table_name, engine, if_exists='append', index=False, schema=schema_name)
    logger.info(f"Successfully ingested {len(df)} rows into {schema_name}.{table_name}.")

def render_yaml_config(yaml_path, context):
    """Read and render a YAML file as a Jinja template using Airflow context."""
    logger.info(f"Rendering YAML config from {yaml_path} with context: {context}")
    with open(yaml_path, "r") as f:
        raw = f.read()
    rendered = Template(raw).render(**context)
    logger.info("YAML config rendered successfully.")
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
        logger.info(f"[DAG: {dag_id}] Ingesting from URI: {uri} with params: {params}")
        source_type = detect_source_type(uri)
        logger.info(f"Detected source type: {source_type}")

        api_params = dict(params) if source_type == 'api' else None
        df = load_data(uri, source_type, params=api_params)
        logger.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

        # Use conn_id and schema from dag params
        conn_id = context['dag'].params['conn_id']
        schema = context['dag'].params['schema']
        logger.info(f"Using Postgres conn_id: {conn_id}, schema: {schema}")
        hook = PostgresHook(postgres_conn_id=conn_id)
        ingest_data(df, table_name, schema, hook)
        logger.info(f"Ingested {len(df)} rows into {schema}.{table_name}")

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
logger.info(f"Loading config from {config_path}")
config = render_yaml_config(config_path, {'data_interval_start': datetime(2025, 7, 1), 'data_interval_end': datetime(2025, 7, 2)})
for source in config.get('data_sources', []):
    dag_name = re.sub(r'\W', '_', source['name'])
    logger.info(f"Registering DAG: ingest_{dag_name}_dag")
    globals()[f"ingest_{dag_name}_dag"] = create_ingest_dag(source)
