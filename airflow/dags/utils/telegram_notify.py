import os
import requests
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from utils.dbt_logger import log


def _get_telegram_creds():
    """
    Ищет токен и chat_id в трех местах по приоритету:
    1. Airflow Variable 'telegram_config' (JSON)
    2. Airflow Connection 'telegram_default'
    3. Окружение (Environment Variables)
    """
    try:
        cfg = Variable.get("telegram_config", deserialize_json=True, default_var=None)
        if cfg and "token" in cfg and "chat_id" in cfg:
            return cfg["token"], cfg["chat_id"]
    except Exception:
        pass

    try:
        conn = BaseHook.get_connection("telegram_default")
        if conn.password and conn.host:
            return conn.password, conn.host
    except Exception:
        pass

    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    return token, chat_id


def send_telegram_message(message: str):
    token, chat_id = _get_telegram_creds()

    if not token or not chat_id:
        log.warning("Telegram credentials not found. Skipping notification.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        log.error(f"Telegram API error: {e}")


def on_failure_callback(context):
    ti = context["task_instance"]
    log_url = ti.log_url
    msg = (
        f"❌ <b>Task Failed</b>\n\n"
        f"<b>DAG:</b> {ti.dag_id}\n"
        f"<b>Task:</b> {ti.task_id}\n"
        f"<b>Error:</b> <code>{str(context.get('exception'))[:150]}...</code>\n"
        f"🔗 <a href='{log_url}'>View Logs</a>"
    )
    send_telegram_message(msg)


def on_success_callback(context):
    ti = context["task_instance"]
    if ti.task_id == "end":
        msg = f"✅ <b>DAG Success:</b> {ti.dag_id}\nStatus: All layers loaded successfuly."
        send_telegram_message(msg)
