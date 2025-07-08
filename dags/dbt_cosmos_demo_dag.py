"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""
import os
from datetime import datetime
from pathlib import Path
from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import ExecutionConfig

DBT_EXECUTABLE_PATH = Path("/usr/local/airflow/dbt_venv/bin/dbt")
DEFAULT_DBT_ROOT_PATH = Path("/usr/local/airflow/dags/dbt")
# Use the environment variable DBT_ROOT_PATH if set, otherwise use the default path
# This allows for the path to be overridden in the Airflow environment
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name=Variable.get("env"),
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="db_conn",
        profile_args={"schema": "stage"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

# [START local_example]
dbt_cosmos_demo_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH.joinpath("cosmos_demo"),
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
        "full_refresh": True,  # used only in dbt commands that support this flag
        "test_flags": ["--no-populate-cache"],  # prevent dbt test from populating cache
    },
    # normal dag parameters
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="dbt_cosmos_demo_dag",
    default_args={"owner": "Teutenberg", "retries": 1},
    tags=["demo", "cosmos", "dbt"],
)
# [END local_example]