from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos.constants import LoadMode
from utils.telegram_notify import on_failure_callback, on_success_callback
from utils.dbt_logger import (
    log_start_callback,
    log_success_callback,
    log_failure_callback,
)


DBT_PROJECT_PATH = Path("/opt/airflow/dbt_core")


def failure_handler(context):
    log_failure_callback(context)
    on_failure_callback(context)


def success_handler(context):
    log_success_callback(context)
    if context["task_instance"].task_id == "end":
        on_success_callback(context)


default_args = {
    "owner": "airflow",
    "on_execute_callback": log_start_callback,  # Используем Loguru для старта
    "on_success_callback": success_handler,
    "on_failure_callback": failure_handler,
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

with DAG(
    dag_id="dbt_data_vault_modular",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:
    start = EmptyOperator(task_id="start")

    def create_layer_group(group_id: str, tag: str):
        return DbtTaskGroup(
            group_id=group_id,
            project_config=ProjectConfig(
                dbt_project_path=DBT_PROJECT_PATH,
            ),
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                select=[f"tag:{tag}"], load_method=LoadMode.DBT_LS
            ),
        )

    staging = create_layer_group("staging_layer", "staging")
    raw_vault = create_layer_group("raw_vault_layer", "raw_vault")
    business_vault = create_layer_group("business_vault_layer", "business_vault")
    marts = create_layer_group("marts_layer", "marts")

    end = EmptyOperator(task_id="end")

    start >> staging >> raw_vault >> business_vault >> marts >> end
