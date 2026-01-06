import sys
from typing import Any
from loguru import logger

LOG_FILE_PATH = "/opt/airflow/logs/dbt_custom.log"

if logger:
    logger.remove()

    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan> - <level>{message}</level>",
        level="INFO",
    )

    logger.add(
        LOG_FILE_PATH,
        rotation="10 MB",
        retention="10 days",
        compression="zip",
        level="DEBUG",
    )
    log = logger
else:
    import logging

    log = logging.getLogger("airflow.task")


def log_start_callback(context: dict[str, Any]) -> None:
    ti = context.get("task_instance")
    log.info(
        f"🚀 [STARTED] Task: {ti.task_id} | DAG: {ti.dag_id} | Execution Date: {context.get('ds')}"
    )


def log_success_callback(context: dict[str, Any]) -> None:
    ti = context.get("task_instance")
    duration = round(ti.duration, 2) if ti.duration else 0
    log.success(f"✅ [SUCCESS] Task: {ti.task_id} | Duration: {duration}s")


def log_failure_callback(context: dict[str, Any]) -> None:
    ti = context.get("task_instance")
    exception = context.get("exception")
    log.error(f"❌ [FAILED] Task: {ti.task_id} | Error: {exception}")


def log_dag_success_callback(context: dict[str, Any]) -> None:
    dag_run = context.get("dag_run")
    log.info(f"🏆 [DAG FINISHED] {dag_run.dag_id} completed successfully!")
