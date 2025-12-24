import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from loguru import logger

logger.add("/opt/airflow/logs/dbt_vault_render.log", rotation="1 MB")

DBT_PROJECT_PATH = Path("/opt/airflow/dbt_core")

profile_config = ProfileConfig(
    profile_name="dbt_core",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

data_vault_dag = DbtDag(
    project_config=ProjectConfig(
        DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        "install_deps": True,
    },
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    dag_id="dbt_data_vault_snowflake",
)

logger.info("DAG 'dbt_data_vault_snowflake' successfully loaded and ready to start.")