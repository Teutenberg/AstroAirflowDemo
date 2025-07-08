"""
Airflow DAG to ingest data from a URI (file, database, or API), auto-detect source type, infer schema, and load into Postgres.
"""
import os
import json
import pandas as pd
import requests
from sqlalchemy import create_engine, inspect
from sqlalchemy.types import Integer, Float, Boolean, String, DateTime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import timedelta
import yaml
import pendulum
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

def get_provider_hook(conn_id):
    """Return a DBAPI hook for the given conn_id, based on conn_type."""
    conn = BaseHook.get_connection(conn_id)
    if conn.conn_type == 'postgres':
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        return PostgresHook(postgres_conn_id=conn_id)
    elif conn.conn_type == 'mysql':
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        return MySqlHook(mysql_conn_id=conn_id)
    # Add more hooks as needed
    else:
        raise ValueError(f"Unsupported conn_type: {conn.conn_type}")

def map_dtype_to_sql(dtype, dialect):
    if str(dtype).startswith('int'):
        sql_type = Integer()
    elif str(dtype).startswith('float'):
        sql_type = Float()
    elif str(dtype) == 'bool':
        sql_type = Boolean()
    elif str(dtype).startswith('datetime'):
        sql_type = DateTime()
    else:
        sql_type = String()
    
    return sql_type.compile(dialect=dialect)

def get_pandas_df(uri, source_type, params=None):
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


def ingest_data(df, table_name, schema_name, hook):
    """Ingests the DataFrame into the database table, evolving schema as needed."""
    logger.info(f"Preparing to ingest data into {schema_name}.{table_name}")
    # Convert any dict columns to JSON strings
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            logger.info(f"Converting column '{col}' dicts to JSON strings.")
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    engine = hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        inspector = inspect(engine)
        # Use dialect-agnostic quoting
        dialect = engine.dialect
        def quote(name):
            return dialect.identifier_preparer.quote(name)
        table_exists = inspector.has_table(table_name, schema=schema_name)
        if table_exists:
            columns = {col['name'] for col in inspector.get_columns(table_name, schema=schema_name)}
            missing_cols = [col for col in df.columns if col not in columns]
            for col in missing_cols:
                sql_type = map_dtype_to_sql(df[col].dtype, engine.dialect)
                logger.info(f"Altering table to add column: {col} {sql_type}")
                alter_sql = f'ALTER TABLE {quote(schema_name)}.{quote(table_name)} ADD COLUMN {quote(col)} {sql_type}' if schema_name else f'ALTER TABLE {quote(table_name)} ADD COLUMN {quote(col)} {sql_type}'
                try:
                    conn.execute(alter_sql)
                except Exception as e:
                    logger.warning(f"Could not add column {col}: {e}")
            logger.info(f"Appending {len(df)} rows to existing table {schema_name}.{table_name}.")
            try:
                df.to_sql(table_name, engine, if_exists='append', index=False, schema=schema_name)
            except TypeError:
                # Some DBs (e.g., SQLite) do not support schema argument
                df.to_sql(table_name, engine, if_exists='append', index=False)
        else:
            logger.info(f"Creating new table {schema_name}.{table_name} and ingesting {len(df)} rows.")
            try:
                df.to_sql(table_name, engine, if_exists='fail', index=False, schema=schema_name)
            except TypeError:
                df.to_sql(table_name, engine, if_exists='fail', index=False)

def create_ingest_dag(source_config, DEFAULT_ARGS=None):
    source_description = source_config.get('description', '')
    source_uri = source_config.get('uri', '')
    source_type = source_config.get('type', '')
    source_params = source_config.get('params', {})
    # Extract dag_configs from source_config
    source_dag_configs = source_config.get('dag_configs', {})
    dag_id = source_dag_configs.get('dag_id', source_config['name'])
    dag_schedule_interval = source_dag_configs.get('schedule', None)
    dag_start_date = pendulum.parse(str(source_dag_configs.get('start_date', '2025-07-01')))
    dag_catchup = source_dag_configs.get('catchup', False)
    dag_concurrency = source_dag_configs.get('concurrency', None)
    dag_max_active_runs = source_dag_configs.get('max_active_runs', None)
    dag_tags = source_dag_configs.get('tags', [])

    def _ingest_data_source(**context):
        """Task to ingest data from the configured source into target."""
        object_name = source_config['name']
        logger.info(f"[DAG: {dag_id}] Ingesting from URI: {source_uri} with params: {source_params}")
        logger.info(f"Detected source type: {source_type}")
        # Render Jinja templates in params if any
        rendered_params = {}
        for k, v in source_params.items():
            if isinstance(v, str) and ('{{' in v and '}}' in v):
                rendered_params[k] = render_jinja_template_with_context(v, **context)
            else:
                rendered_params[k] = v
        logger.info(f"Rendered params: {rendered_params}")
        dag = context.get('dag')
        jinja_env = dag.get_template_env() if dag else None
        df = get_pandas_df(source_uri, source_type, params=rendered_params)
        logger.info(f"Loaded {len(df)} rows with columns: {list(df.columns)}")

        # Use conn_id and schema from dag params
        conn_id = context['dag'].params['conn_id']
        schema = context['dag'].params['schema']
        logger.info(f"Using conn_id: {conn_id}, schema: {schema}")
        hook = get_provider_hook(conn_id)
        ingest_data(df, object_name, schema, hook)
        logger.info(f"Ingested {len(df)} rows into {schema}.{object_name}")

    dag = DAG(
        dag_id,
        default_args=DEFAULT_ARGS,
        description=source_description,
        schedule_interval=dag_schedule_interval,
        start_date=dag_start_date,
        catchup=dag_catchup,
        tags=dag_tags,
        concurrency=dag_concurrency,
        max_active_runs=dag_max_active_runs,
        params={
            'conn_id': source_config.get('conn_id', 'default_conn'),
            'schema': source_config.get('schema', 'demo'),
        }
    )

    PythonOperator(
        task_id='ingest_data_sources',
        python_callable=_ingest_data_source,
        dag=dag,
    )
    return dag

def render_jinja_template_with_context(template_str, **context):
    """Render a Jinja template string using Airflow's Jinja environment and task context."""
    dag = context.get('dag')
    if not dag:
        raise ValueError("DAG object must be present in context to render Jinja template.")
    jinja_env = dag.get_template_env()
    template = jinja_env.from_string(template_str)
    return template.render(**context)

# Render the YAML config at module load to create DAGs dynamically
# Use absolute path to avoid double 'landing/landing' bug
# Default args for the DAG
DEFAULT_ARGS = {
    'owner': 'Teutenberg',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

config_path = os.path.join(os.path.dirname(__file__), "ingest_data_sources.yml")
logger.info(f"Loading config from {config_path}")

with open(config_path, "r") as f:
    raw = f.read()
    config = yaml.safe_load(raw)

for source in config.get('data_sources', []):
    dag_config = source['dag_configs']
    dag_id = dag_config.get('dag_id', source['name'])
    logger.info(f"Registering DAG: {dag_id}")
    globals()[dag_id] = create_ingest_dag(source, DEFAULT_ARGS)
