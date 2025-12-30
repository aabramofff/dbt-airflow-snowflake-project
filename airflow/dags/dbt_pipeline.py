import os
import logging
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/opt/airflow/dbt_core")

def log_status(status, context):
    task_id = context['task_instance'].task_id
    logging.info(f"--- Task {task_id}: {status} at {datetime.now()} ---")

default_args = {
    "owner": "airflow",
    "on_execute_callback": lambda context: log_status("STARTED", context),
    "on_success_callback": lambda context: log_status("SUCCESS", context),
    "on_failure_callback": lambda context: log_status("FAILED", context),
}

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

def run_dbt_deps():
    os.system(f"{execution_config.dbt_executable_path} deps --project-dir {DBT_PROJECT_PATH}")

with DAG(
    dag_id="dbt_data_vault_modular",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args, 
) as dag:

    start = EmptyOperator(task_id="start")
    
    install_deps = PythonOperator(
        task_id="install_dbt_dependencies",
        python_callable=run_dbt_deps
    )

    def create_layer_group(group_id: str, tag: str):
        return DbtTaskGroup(
            group_id=group_id,
            project_config=ProjectConfig(
                dbt_project_path = DBT_PROJECT_PATH,
                partial_parse = True,
            ),
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                select=[f"tag:{tag}"],
                load_method=LoadMode.DBT_LS
            )
        )

    seeds = create_layer_group("dbt_seeds", "seed") 
    staging = create_layer_group("staging_layer", "staging")
    raw_vault = create_layer_group("raw_vault_layer", "raw_vault")
    business_vault = create_layer_group("business_vault_layer", "business_vault")
    marts = create_layer_group("marts_layer", "marts")

    end = EmptyOperator(task_id="end")

    start >> install_deps >> seeds >> staging >> raw_vault >> business_vault >> marts >> end
