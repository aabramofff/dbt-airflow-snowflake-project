import os
import logging

try:
    import requests
except ImportError:
    requests = None

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram_message(message: str):
    if not requests:
        logging.error("Requests library not found. Cannot send Telegram message.")
        return
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram credentials missing.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Telegram error: {e}")


def on_failure_callback(context):
    ti = context["task_instance"]
    msg = (
        f"❌ <b>Task Failed</b>\n\n"
        f"<b>DAG:</b> {ti.dag_id}\n"
        f"<b>Task:</b> {ti.task_id}\n"
        f"<b>Error:</b> <code>{str(context.get('exception'))[:200]}</code>"
    )
    send_telegram_message(msg)


def on_success_callback(context):
    if context["task_instance"].task_id == "end":
        msg = f"✅ <b>DAG Success:</b> {context['task_instance'].dag_id} completed!"
        send_telegram_message(msg)
