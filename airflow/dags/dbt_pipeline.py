import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import LoadMode

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

with DAG(
    dag_id="dbt_data_vault_modular",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    def create_layer_group(group_id: str, tag: str):
        return DbtTaskGroup(
            group_id=group_id,
            project_config=ProjectConfig(DBT_PROJECT_PATH),
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                select=[f"tag:{tag}"],
                load_method=LoadMode.DBT_LS
            )
        )

    staging = create_layer_group("staging_layer", "staging")
    raw_vault = create_layer_group("raw_vault_layer", "raw_vault")
    business_vault = create_layer_group("business_vault_layer", "business_vault")
    marts = create_layer_group("marts_layer", "marts")

    staging >> raw_vault >> business_vault >> marts
