import logging
import os
import re
import json
import csv
import hashlib
import base64
import io
import random
import time
import threading
import uuid
import sqlite3
from datetime import date, datetime
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path

import requests
from flask import Flask, g, jsonify, redirect, render_template, request, session, url_for
from flask_session import Session

try:
    import matplotlib

    matplotlib.use("Agg")
    from matplotlib import pyplot as plt

    MATPLOTLIB_AVAILABLE = True
    MATPLOTLIB_IMPORT_ERROR = ""
except Exception as import_error:  # pragma: no cover - depends on runtime environment
    plt = None
    MATPLOTLIB_AVAILABLE = False
    MATPLOTLIB_IMPORT_ERROR = str(import_error)

from db_engine import (
    execute_read_only_sql,
    fetch_schema_metadata,
    fetch_track_packages_data,
    format_schema_for_prompt,
    set_current_account_id,
)
from mcp_bridge import execute_read_only_sql_tool, get_schema_metadata_tool


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "notifii-dev-secret-key")
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_PERMANENT"] = False
Session(app)

LOG_PATH = Path(__file__).resolve().parent / "login_page.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger("login_page")
REQUEST_STEP_LOGGING_ENABLED = os.getenv("CHATBOT_REQUEST_STEP_LOGGING", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def _ensure_login_logger_file_handler():
    """Attach a dedicated file handler so login_app logs always reach login_page.log."""
    target_path = str(LOG_PATH.resolve())
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler_path = str(Path(getattr(handler, "baseFilename", "")).resolve())
            if handler_path == target_path:
                return

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)


_ensure_login_logger_file_handler()

if not MATPLOTLIB_AVAILABLE:
    logger.warning("Chart rendering disabled because matplotlib import failed: %s", MATPLOTLIB_IMPORT_ERROR)

SQL_TABLE_CSV_PATH = Path(__file__).resolve().parent / "sql_query_table.csv"
SQL_TABLE_CSV_COLUMNS = [
    "timestamp",
    "level",
    "logger",
    "account_id",
    "user_query",
    "generated_sql",
    "status",
    "rows",
    "response",
    "parse_error",
    "raw_entry",
]
SQL_TABLE_RESET_MARKER_PATH = Path(__file__).resolve().parent / ".sql_query_table_last_reset"
SQL_TABLE_WRITE_LOCK = threading.Lock()
SQL_TABLE_WRITE_RETRY_COUNT = max(1, int(os.getenv("CHATBOT_SQL_TABLE_WRITE_RETRY_COUNT", "3")))
SQL_TABLE_WRITE_RETRY_DELAY_S = max(
    0.01,
    float(os.getenv("CHATBOT_SQL_TABLE_WRITE_RETRY_DELAY_S", "0.05")),
)
SQL_TABLE_RESET_DAYS = max(1, int(os.getenv("CHATBOT_SQL_TABLE_RESET_DAYS", "7")))


def _read_sql_table_last_reset_epoch():
    """Return last reset epoch seconds from marker file, if available."""
    try:
        if SQL_TABLE_RESET_MARKER_PATH.exists():
            raw_value = SQL_TABLE_RESET_MARKER_PATH.read_text(encoding="utf-8").strip()
            if raw_value:
                return float(raw_value)
    except (OSError, ValueError) as error:
        logger.warning("Unable to read SQL CSV reset marker %s: %s", SQL_TABLE_RESET_MARKER_PATH, error)
    return None


def _write_sql_table_last_reset_epoch(epoch_seconds):
    """Persist last reset epoch seconds to marker file."""
    try:
        SQL_TABLE_RESET_MARKER_PATH.write_text(str(float(epoch_seconds)), encoding="utf-8")
    except OSError as error:
        logger.warning("Unable to write SQL CSV reset marker %s: %s", SQL_TABLE_RESET_MARKER_PATH, error)


def _ensure_sql_table_csv_header():
    """Create SQL CSV log file header and reset file when older than configured reset window."""
    needs_header = not SQL_TABLE_CSV_PATH.exists() or SQL_TABLE_CSV_PATH.stat().st_size == 0
    if not needs_header:
        now_epoch = time.time()
        last_reset_epoch = _read_sql_table_last_reset_epoch()
        if last_reset_epoch is None:
            try:
                # Fallback to file modified time when marker is missing.
                last_reset_epoch = SQL_TABLE_CSV_PATH.stat().st_mtime
            except OSError:
                last_reset_epoch = now_epoch

        file_age_days = (now_epoch - last_reset_epoch) / 86400.0
        if file_age_days >= SQL_TABLE_RESET_DAYS:
            logger.info(
                "Resetting %s after %.2f day(s) (threshold=%s day(s)).",
                SQL_TABLE_CSV_PATH,
                file_age_days,
                SQL_TABLE_RESET_DAYS,
            )
            needs_header = True

    if not needs_header:
        return

    with SQL_TABLE_CSV_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=SQL_TABLE_CSV_COLUMNS)
        writer.writeheader()
    _write_sql_table_last_reset_epoch(time.time())


def _append_sql_table_row(account_id, user_query, generated_sql, status, rows_count, answer):
    """Append one chatbot SQL interaction row into CSV table format."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

    row = {
        "timestamp": timestamp,
        "level": "INFO",
        "logger": "chatbot_sql",
        "account_id": "" if account_id is None else str(account_id),
        "user_query": "" if user_query is None else str(user_query),
        "generated_sql": "" if generated_sql is None else str(generated_sql),
        "status": "" if status is None else str(status),
        "rows": "" if rows_count is None else str(rows_count),
        "response": "" if answer is None else str(answer),
        "parse_error": "",
        "raw_entry": "",
    }

    for attempt in range(1, SQL_TABLE_WRITE_RETRY_COUNT + 1):
        try:
            with SQL_TABLE_WRITE_LOCK:
                _ensure_sql_table_csv_header()
                with SQL_TABLE_CSV_PATH.open("a", newline="", encoding="utf-8") as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=SQL_TABLE_CSV_COLUMNS)
                    writer.writerow(row)
            return True
        except PermissionError as error:
            if attempt >= SQL_TABLE_WRITE_RETRY_COUNT:
                logger.warning(
                    "CSV log write skipped after %s attempt(s) due to file lock on %s: %s",
                    attempt,
                    SQL_TABLE_CSV_PATH,
                    error,
                )
                return False
            time.sleep(SQL_TABLE_WRITE_RETRY_DELAY_S)
        except OSError as error:
            logger.warning("CSV log write failed for %s: %s", SQL_TABLE_CSV_PATH, error)
            return False

    return False


_ensure_sql_table_csv_header()


def _request_path_tag():
    """Return stable path tag for request timing logs."""
    try:
        return f"{request.method} {request.path}"
    except RuntimeError:
        return "NO_REQUEST_CONTEXT"


def begin_request_timing():
    """Initialize per-request timing context."""
    g.request_id = uuid.uuid4().hex[:8]
    g.request_started_at = time.perf_counter()
    g.last_step_at = g.request_started_at
    g.request_steps = []
    logger.info("[REQ %s] START %s", g.request_id, _request_path_tag())


def mark_step(step_name, **details):
    """Record elapsed and delta timings (seconds) for a request step."""
    if not hasattr(g, "request_started_at"):
        return

    now = time.perf_counter()
    elapsed_s = now - g.request_started_at
    delta_s = now - g.last_step_at
    g.last_step_at = now

    detail_items = []
    for key, value in details.items():
        detail_items.append(f"{key}={value}")
    detail_text = "; ".join(detail_items) if detail_items else ""

    g.request_steps.append(
        {
            "step": step_name,
            "elapsed_s": elapsed_s,
            "delta_s": delta_s,
            "detail": detail_text,
        }
    )

    if REQUEST_STEP_LOGGING_ENABLED:
        if detail_text:
            logger.info(
                "[REQ %s] STEP %s | +%.3fs | %.3fs total | %s",
                g.request_id,
                step_name,
                delta_s,
                elapsed_s,
                detail_text,
            )
        else:
            logger.info(
                "[REQ %s] STEP %s | +%.3fs | %.3fs total",
                g.request_id,
                step_name,
                delta_s,
                elapsed_s,
            )


def _format_request_timeline(steps):
    """Compact timeline text for terminal summary."""
    formatted = []
    for item in steps:
        part = f"{item['step']}:+{item['delta_s']:.3f}s"
        if item.get("detail"):
            part += f"[{item['detail']}]"
        formatted.append(part)
    return " -> ".join(formatted)


def _step_delta_s(steps, step_name):
    """Return delta seconds for a specific step name."""
    for item in steps:
        if item.get("step") == step_name:
            return item.get("delta_s", 0.0)
    return 0.0


@app.before_request
def _before_request_timing():
    begin_request_timing()


@app.after_request
def _after_request_timing(response):
    if hasattr(g, "request_started_at"):
        total_s = time.perf_counter() - g.request_started_at
        steps = getattr(g, "request_steps", [])
        timeline = _format_request_timeline(steps)
        slowest = max(steps, key=lambda item: item["delta_s"], default=None)
        if slowest:
            slowest_text = f"{slowest['step']} ({slowest['delta_s']:.3f}s)"
        else:
            slowest_text = "none"

        logger.info(
            "[REQ %s] END %s | status=%s | total=%.3fs | slowest=%s | timeline=%s",
            getattr(g, "request_id", "n/a"),
            _request_path_tag(),
            response.status_code,
            total_s,
            slowest_text,
            timeline or "no-steps-recorded",
        )

        if request.path == "/chatbot/ask" and request.method == "POST":
            top_steps = sorted(steps, key=lambda item: item.get("delta_s", 0.0), reverse=True)[:3]
            top_steps_text = ", ".join(
                f"{item.get('step')}={item.get('delta_s', 0.0):.3f}s" for item in top_steps
            )
            sql_generated_s = _step_delta_s(steps, "chatbot_ask_sql_generated")
            logger.info(
                "CHATBOT_TIMING | req=%s | ui_wait_s=%.3f | status=%s | intent_s=%.3f | rewrite_s=%.3f | sql_gen_s=%.3f | fields_s=%.3f | answer_s=%.3f | db_exec_s=%.3f | total_s=%.3f | slowest=%s | top3=%s",
                getattr(g, "request_id", "n/a"),
                total_s,
                response.status_code,
                _step_delta_s(steps, "chatbot_ask_intent_classified"),
                _step_delta_s(steps, "chatbot_ask_query_rewritten"),
                sql_generated_s,
                _step_delta_s(steps, "chatbot_ask_requested_fields_inferred"),
                _step_delta_s(steps, "chatbot_ask_answer_generated"),
                _step_delta_s(steps, "chatbot_ask_sql_executed"),
                total_s,
                slowest_text,
                top_steps_text,
            )
            response.headers["X-SQL-Generated-S"] = f"{sql_generated_s:.3f}"

            cache_key = getattr(g, "chatbot_response_cache_key", "")
            if cache_key and response.status_code == 200:
                response_payload = response.get_json(silent=True)
                if isinstance(response_payload, dict) and response_payload.get("status") == "ok":
                    _set_cached_query_response(cache_key, response_payload)

        response.headers["X-Request-Id"] = str(getattr(g, "request_id", "n/a"))
        response.headers["X-Request-Total-S"] = f"{total_s:.3f}"
        response.headers["X-Request-Slowest-Step"] = slowest_text
    return response


@app.teardown_request
def _teardown_request_timing(error):
    if error is not None and hasattr(g, "request_started_at"):
        total_s = time.perf_counter() - g.request_started_at
        logger.exception(
            "[REQ %s] ERROR %s | total=%.3fs | error=%s",
            getattr(g, "request_id", "n/a"),
            _request_path_tag(),
            total_s,
            error,
        )

LOGIN_API_URL = "https://portal.ntfdev2.com/api/track7/app-login.php"
QWEN_API_URL = os.getenv("QWEN_API_URL", "http://localhost:11435/api/generate")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen2.5:3b")
QWEN_TIMEOUT = int(os.getenv("QWEN_TIMEOUT", "45"))
EXPLICIT_TABLE_QWEN_TIMEOUT = int(os.getenv("CHATBOT_EXPLICIT_TABLE_QWEN_TIMEOUT", "12"))
SECONDARY_QWEN_TIMEOUT = max(5, min(int(os.getenv("CHATBOT_SECONDARY_QWEN_TIMEOUT", "12")), QWEN_TIMEOUT))
ADAPTIVE_SQL_TIMEOUT_ENABLED = os.getenv("CHATBOT_ADAPTIVE_SQL_TIMEOUT_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ADAPTIVE_SQL_SIMPLE_TIMEOUT = max(6, min(int(os.getenv("CHATBOT_ADAPTIVE_SQL_SIMPLE_TIMEOUT", "16")), QWEN_TIMEOUT))
ADAPTIVE_SQL_MEDIUM_TIMEOUT = max(8, min(int(os.getenv("CHATBOT_ADAPTIVE_SQL_MEDIUM_TIMEOUT", "26")), QWEN_TIMEOUT))
SQL_GEN_CACHE_ENABLED = os.getenv("CHATBOT_SQL_GEN_CACHE_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
SQL_GEN_CACHE_TTL_S = max(5, int(os.getenv("CHATBOT_SQL_GEN_CACHE_TTL_S", "300")))
SQL_GEN_CACHE_MAX_ITEMS = max(20, int(os.getenv("CHATBOT_SQL_GEN_CACHE_MAX_ITEMS", "500")))
SQL_GEN_CACHE = {}
SQL_GEN_CACHE_LOCK = threading.Lock()
QUERY_RESPONSE_CACHE_ENABLED = os.getenv("CHATBOT_QUERY_RESPONSE_CACHE_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
QUERY_RESPONSE_CACHE_TTL_S = max(5, int(os.getenv("CHATBOT_QUERY_RESPONSE_CACHE_TTL_S", "180")))
QUERY_RESPONSE_CACHE_MAX_ITEMS = max(20, int(os.getenv("CHATBOT_QUERY_RESPONSE_CACHE_MAX_ITEMS", "800")))
QUERY_RESPONSE_CACHE = {}
QUERY_RESPONSE_CACHE_LOCK = threading.Lock()
CHART_PAYLOAD_CACHE_ENABLED = os.getenv("CHATBOT_CHART_PAYLOAD_CACHE_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
CHART_PAYLOAD_CACHE_TTL_S = max(5, int(os.getenv("CHATBOT_CHART_PAYLOAD_CACHE_TTL_S", "180")))
CHART_PAYLOAD_CACHE_MAX_ITEMS = max(20, int(os.getenv("CHATBOT_CHART_PAYLOAD_CACHE_MAX_ITEMS", "300")))
CHART_PAYLOAD_CACHE = {}
CHART_PAYLOAD_CACHE_LOCK = threading.Lock()
TABLE_DIRECT_FAST_PATH_ENABLED = os.getenv("CHATBOT_TABLE_DIRECT_FAST_PATH_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
EXPLICIT_TABLE_MAX_PROMPT_TABLES = max(
    1,
    min(int(os.getenv("CHATBOT_EXPLICIT_TABLE_MAX_PROMPT_TABLES", "2")), 5),
)
EXPLICIT_TABLE_SKIP_SECONDARY_LLM = os.getenv("CHATBOT_EXPLICIT_TABLE_SKIP_SECONDARY_LLM", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
USE_LLM_FOR_SMALL_TALK = os.getenv("USE_LLM_FOR_SMALL_TALK", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ROUTING_DEBUG = os.getenv("ROUTING_DEBUG", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
SERVER_SIDE_CHART_IMAGE_RENDER = os.getenv("CHATBOT_SERVER_SIDE_CHART_IMAGE_RENDER", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
MCP_ENABLED = os.getenv("MCP_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
USE_DYNAMIC_SELECTOR_ONLY = os.getenv("CHATBOT_USE_DYNAMIC_SELECTOR_ONLY", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
USE_ACCOUNT_DATASET_EXECUTION = os.getenv("CHATBOT_USE_ACCOUNT_DATASET_EXECUTION", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ACCOUNT_DATASET_MAX_ROWS = max(100, min(int(os.getenv("CHATBOT_ACCOUNT_DATASET_MAX_ROWS", "2000")), 10000))


def fetch_schema_metadata_for_chatbot(force_refresh=False):
    """Return schema metadata through MCP tool when enabled, else direct DB helper."""
    if not MCP_ENABLED:
        return fetch_schema_metadata(force_refresh=force_refresh)

    tool_response = get_schema_metadata_tool(force_refresh=force_refresh)
    schema_map = tool_response.get("schema_map", {}) if isinstance(tool_response, dict) else {}
    if isinstance(schema_map, dict):
        return schema_map
    return {}


def execute_read_only_sql_for_chatbot(sql_text, max_rows=100):
    """Execute SQL via MCP tool when enabled, else direct DB helper."""
    if not MCP_ENABLED:
        return execute_read_only_sql(sql_text, max_rows=max_rows)

    tool_response = execute_read_only_sql_tool(sql_text=sql_text, max_rows=max_rows)
    if not isinstance(tool_response, dict):
        return [], "invalid_mcp_tool_response"

    rows = tool_response.get("rows", [])
    status = str(tool_response.get("status", "invalid_mcp_tool_response"))
    if not isinstance(rows, list):
        rows = []
    return rows, status
OUT_OF_DB_RESPONSE = os.getenv(
    "CHATBOT_OUT_OF_DB_RESPONSE",
    "I cannot respond because you do not have sufficient data.",
)
QUERY_REPHRASE_RESPONSE = os.getenv(
    "CHATBOT_QUERY_REPHRASE_RESPONSE",
    "I don't have access to answer your question please rephrase your question.",
)
INTENT_CLARIFICATION_RESPONSE = os.getenv(
    "CHATBOT_INTENT_CLARIFICATION_RESPONSE",
    "I need a bit more information to fetch accurate data. Please share the table name or module, what fields you need, and any time range/filter.",
)
REQUEST_COMPLETED_RESPONSE = os.getenv("CHATBOT_REQUEST_COMPLETED_RESPONSE", "Request completed.")
CONFIRMATION_INVALID_RESPONSE = os.getenv(
    "CHATBOT_CONFIRMATION_INVALID_RESPONSE",
    "Please reply with yes, no, or ok.",
)
FOLLOWUP_CONTEXT_MISSING_RESPONSE = os.getenv(
    "CHATBOT_FOLLOWUP_CONTEXT_MISSING_RESPONSE",
    "I do not have previous record context to show. Please ask your data question again.",
)
SENSITIVE_QUERY_RESPONSE = os.getenv(
    "CHATBOT_SENSITIVE_QUERY_RESPONSE",
    "I cannot assist with passwords or other sensitive account credentials.",
)
DEFAULT_GREETING_REPLIES = [
    "Hello! I can help you with your account package data questions.",
    "Hi there! Ask me anything about your account package records.",
    "Good to see you. I can help with package tracking, counts, and delivery dates.",
    "Welcome! I am ready to help with your package data queries.",
    "Hello! I can fetch and summarize your account package details.",
]
TRACK_PACKAGES_COLUMNS = [
    "package_id",
    "account_id",
    "tracking_number",
    "shipping_carrier",
    "recipient_id",
    "mailroom_id",
    "group_id",
    "compartment_id",
    "login_module",
    "recipient_name",
    "recipient_address1",
    "date_received",
    "date_received_yyyymm",
    "date_expires",
    "date_pickedup",
    "handling",
    "shelf",
    "sender",
    "service_type",
    "package_condition",
    "package_type",
    "custom_message",
    "weight",
    "dimensions",
    "po_number",
    "tag_number",
    "esignature_url",
    "login_carrier_id",
    "login_user_id",
    "logout_code_id",
    "logout_user_id",
    "logout_recipient_id",
    "staff_note",
    "change_history",
    "shorturl_login",
    "shorturl_logout",
    "ocr_triggered",
    "package_sequence",
    "locker_pin_code",
]
CORE_RECIPIENTS_COLUMNS = [
    "recipient_id",
    "account_id",
    "first_name",
    "preferred_first_name",
    "last_name",
    "email",
    "cellphone",
    "cellphone_digits",
    "cellphone_formatted",
    "wireless_carrier",
    "sms_email",
    "address1",
    "address2",
    "recipient_title",
    "recipient_type",
    "recipient_status",
    "recipient_notes",
    "requires_handicap_locker",
    "idnumber",
    "username",
    "pass",
    "date_added",
    "date_removed",
    "last_login",
    "login_count",
    "last_ip_address",
    "external_db_id",
    "data_integration_sync_status",
    "email_bounced",
    "cellphone_bounced",
    "email_bounce_reason",
    "cellphone_bounce_reason",
    "email_bounced_date",
    "cellphone_bounced_date",
    "phone_type",
    "ps_alert",
    "ps_alert_override_optout",
    "recipient_profile_image",
    "send_track_nonmarketing_email",
    "send_connect_nonmarketing_email",
    "send_connect_marketing_email",
    "send_track_nonmarketing_text",
    "send_connect_nonmarketing_text",
    "send_connect_marketing_text",
    "send_track_nonmarketing_push",
    "send_connect_nonmarketing_push",
    "send_connect_marketing_push",
    "stop_track_nonmarketing_email",
    "stop_connect_nonmarketing_email",
    "stop_connect_marketing_email",
    "stop_track_nonmarketing_text",
    "stop_connect_nonmarketing_text",
    "stop_connect_marketing_text",
    "stop_track_nonmarketing_push",
    "stop_connect_nonmarketing_push",
    "stop_connect_marketing_push",
    "move_in_date",
    "move_out_date",
    "lease_start_date",
    "lease_end_date",
    "birth_date",
    "special_track_instructions_flag",
    "special_track_instructions",
    "vacation_status",
    "vacation_start_date",
    "vacation_end_date",
    "send_pkg_login_notification",
    "send_pkg_logout_notification",
    "sent_welcome_sms",
    "sent_opt_in_sms",
    "date_modified",
    "send_checkout_nonmarketing_email",
    "send_checkout_nonmarketing_text",
    "send_checkout_nonmarketing_push",
    "stop_checkout_nonmarketing_email",
    "stop_checkout_nonmarketing_text",
    "stop_checkout_nonmarketing_push",
    "external_notification_id",
    "package_portal_shortlink",
]
DEFAULT_STATIC_SUPPORTED_QUERY_TABLES = [
    "checkout_assets",
    "checkout_categories",
    "checkout_checkin_codes",
    "checkout_checkout_pictures",
    "checkout_checkouts",
    "checkout_conditions",
    "checkout_facilities",
    "checkout_guest_pictures",
    "checkout_guest_unsubscribes",
    "checkout_guests",
    "checkout_item_pictures",
    "checkout_items",
    "checkout_notification_messages",
    "checkout_notifications",
    "checkout_reservations",
    "checkout_settings",
    "checkout_setup_checklist",
    "checkout_shelves",
    "checkout_templates",
    "checkout_templates_production",
    "connect_app_dynamic_configurations",
    "connect_app_static_configurations",
    "connect_automated_message_analytics",
    "connect_automated_message_attachments",
    "connect_automated_message_individuals",
    "connect_automated_message_mailqueue",
    "connect_automated_message_mailqueue_attachments",
    "connect_automated_message_master_attachments",
    "connect_automated_message_masters",
    "connect_automated_messages",
    "connect_autoresponders",
    "connect_chat_messages",
    "connect_chat_rooms",
    "connect_community_post_attachments",
    "connect_community_post_comments",
    "connect_community_post_individuals",
    "connect_community_post_masters",
    "connect_event_attachments",
    "connect_event_individuals",
    "connect_event_masters",
    "connect_individual_messages",
    "connect_mailqueue",
    "connect_mailqueue_attachments",
    "connect_manager_post_analytics",
    "connect_manager_post_attachments",
    "connect_manager_post_comments",
    "connect_manager_post_individuals",
    "connect_manager_post_masters",
    "connect_manager_post_replies",
    "connect_master_messages",
    "connect_message_approvals",
    "connect_prospect_unsubscribes",
    "connect_prospects",
    "connect_recipient_analytics",
    "connect_recipient_groups",
    "connect_sessions",
    "connect_settings",
    "connect_setup_checklist",
    "connect_stylesheets",
    "connect_template_attachments",
    "connect_template_folders",
    "connect_templates",
    "connect_templates_production",
    "connect_test_message_attachments",
    "connect_test_messages",
    "connect_texting_widgets",
    "connect_tv_configurations",
    "connect_tv_sessions",
    "connect_usage",
    "content_blog_categories",
    "content_blog_tags",
    "content_blogs",
    "content_faqs",
    "content_faqs_categories",
    "content_kb",
    "content_kb_categories",
    "content_kb_subcategories",
    "content_kb_tags",
    "core_account_attributes",
    "core_account_billing",
    "core_account_closure_reasons",
    "core_account_counts",
    "core_account_documents",
    "core_account_limits",
    "core_account_modules",
    "core_account_monthly_stats",
    "core_account_onboarding_status",
    "core_account_onboarding_steps",
    "core_account_settings",
    "core_account_utm_parameters",
    "core_accounts",
    "core_countries",
    "core_email_domain_usage",
    "core_email_domains",
    "core_failed_logins",
    "core_favorite_menus",
    "core_favorite_menus_new",
    "core_invoice_items",
    "core_invoices",
    "core_login_history",
    "core_menus",
    "core_menus_new",
    "core_mfa_codes",
    "core_migration_rsc_files_to_s3",
    "core_recipient_notifications",
    "core_recipient_types",
    "core_recipient_unsubscribes",
    "core_recipients",
    "core_rsc_container_urls",
    "core_scheduled_reports",
    "core_sendgrid_events",
    "core_sendgrid_json_data",
    "core_sessions",
    "core_short_urls",
    "core_sms_log",
    "core_sso",
    "core_sso_user_type_mappings",
    "core_states",
    "core_system_messages",
    "core_timezones",
    "core_twilio_blocks",
    "core_twilio_messaging_services",
    "core_twilio_phone_numbers",
    "core_twilio_reply_data",
    "core_twilio_standard_registrations",
    "core_twilio_starter_registrations",
    "core_user_activities",
    "core_user_activities_archive",
    "core_user_marketing",
    "core_user_passwords",
    "core_user_settings",
    "core_user_types",
    "core_users",
    "core_wireless_carriers",
    "corporate_accounts",
    "corporate_attribute_names",
    "corporate_attribute_values",
    "corporate_automatic_bcc",
    "corporate_documents",
    "corporate_failed_logins",
    "corporate_mfa_codes",
    "corporate_property_groups",
    "corporate_sessions",
    "corporate_settings",
    "corporate_user_activities",
    "corporate_user_passwords",
    "corporate_user_types",
    "corporate_users",
    "guest_ip_date",
    "locker_account_sessions",
    "locker_account_sizes",
    "locker_activities",
    "locker_alerts",
    "locker_app_designs",
    "locker_app_versions",
    "locker_breakage_logs",
    "locker_carrier_sessions",
    "locker_carriers",
    "locker_compartments",
    "locker_design_templates",
    "locker_door_incidents",
    "locker_global_sizes",
    "locker_groups",
    "locker_openmylocker",
    "locker_pings",
    "locker_pubnub_notifications",
    "locker_recipient_sessions",
    "locker_reservations",
    "locker_settings",
    "locker_staff_login_qrcodes",
    "locker_staff_sessions",
    "locker_technician_sessions",
    "locker_technician_validation_logs",
    "locker_technician_validations",
    "locker_technicians",
    "locker_templates",
    "locker_tower_configurations",
    "locker_towers",
    "track_logout_codes",
    "track_mailqueue",
    "track_mailrooms",
    "track_notification_messages",
    "track_notifications",
    "track_notifications_archived_2018nov30",
    "track_ocr",
    "track_package_conditions",
    "track_package_pictures",
    "track_package_pictures_pro",
    "track_package_types",
    "track_packages",
    "track_packages_archive",
    "track_receipt_scan_batches",
    "track_receipt_scan_items",
    "track_receipt_scan_pictures",
    "track_senders",
    "track_service_types",
    "track_settings",
    "track_setup_checklist",
    "track_shelves",
    "track_shipping_carriers",
    "track_special_handlings",
    "track_templates",
    "track_tv_configurations",
    "track_tv_sessions",
]


def _load_supported_query_tables():
    """Load supported query tables from env; defaults are static and explicit."""
    default_tables = list(DEFAULT_STATIC_SUPPORTED_QUERY_TABLES)
    raw_value = str(os.getenv("CHATBOT_SUPPORTED_QUERY_TABLES", "") or "").strip()
    if not raw_value:
        return tuple(default_tables)

    parsed_tables = []
    try:
        loaded = json.loads(raw_value)
        if isinstance(loaded, list):
            parsed_tables = [str(item).strip().lower() for item in loaded if str(item).strip()]
    except ValueError:
        parsed_tables = [piece.strip().lower() for piece in raw_value.split(",") if piece.strip()]

    if not parsed_tables:
        return tuple(default_tables)

    unique_tables = []
    seen = set()
    for table_name in parsed_tables:
        if table_name in seen:
            continue
        seen.add(table_name)
        unique_tables.append(table_name)

    for required_table in ("track_packages", "core_recipients", "guest_ip_date"):
        if required_table not in seen:
            unique_tables.append(required_table)

    return tuple(unique_tables)


SUPPORTED_QUERY_TABLES = _load_supported_query_tables()
def _get_runtime_supported_tables(schema_map):
    """Return static supported tables (prefix discovery removed by design)."""
    _ = schema_map
    resolved_tables = []
    seen = set()
    for table_name in SUPPORTED_QUERY_TABLES:
        normalized = str(table_name).strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        resolved_tables.append(normalized)
    return tuple(resolved_tables)


SUPPORTED_TABLE_DEFAULT_COLUMNS = {
    "track_packages": TRACK_PACKAGES_COLUMNS,
    "core_recipients": CORE_RECIPIENTS_COLUMNS,
}

ALL_SUPPORTED_COLUMNS = []
for _table_name in SUPPORTED_QUERY_TABLES:
    for _column_name in SUPPORTED_TABLE_DEFAULT_COLUMNS.get(_table_name, []):
        if _column_name not in ALL_SUPPORTED_COLUMNS:
            ALL_SUPPORTED_COLUMNS.append(_column_name)

if not ALL_SUPPORTED_COLUMNS:
    ALL_SUPPORTED_COLUMNS = TRACK_PACKAGES_COLUMNS + [
        column for column in CORE_RECIPIENTS_COLUMNS if column not in TRACK_PACKAGES_COLUMNS
    ]
TABLE_VIEW_SAFE_LIMIT = max(1, min(int(os.getenv("CHATBOT_TABLE_VIEW_SAFE_LIMIT", "100")), 100))

DEFAULT_GREETING_PATTERNS = [
    r"^hi$",
    r"^hello$",
    r"^hey$",
    r"^good\s+morning$",
    r"^good\s+evening$",
    r"^good\s+night$",
    r"^good\s+afternoon$",
    r"^hello\s+there$",
]
DEFAULT_HELP_TERMS = ["help", "query", "queries", "question", "questions", "assist", "support"]
DEFAULT_HELP_SUBJECT_TERMS = ["you", "your", "bot", "chatbot", "assistant"]
DEFAULT_HELP_ACTION_TERMS = ["can", "do", "capable", "capabilities", "features", "function", "functions", "work"]
DEFAULT_HELP_CONTEXT_TERMS = ["help", "support", "assist", "capabilities", "capability", "features"]
DEFAULT_DATE_TOKEN_TERMS = ["which date", "what date", "on which date", "delivery date", "date", "when"]
DEFAULT_PEAK_TERMS = ["most", "more", "highest", "maximum", "max", "top", "how many", "count", "number"]
DEFAULT_DISPLAY_MODE_PATTERNS = [
    r"\btable format\b",
    r"\bin table\b",
    r"\bshow in table\b",
    r"\bdisplay in table\b",
    r"\bshow as table\b",
    r"\bconvert to table\b",
    r"\btable view\b",
]
DEFAULT_CHART_MODE_PATTERNS = [
    r"\bchart\b",
    r"\bcharts\b",
    r"\bgraph\b",
    r"\bplot\b",
    r"\bvisuali[sz]e\b",
    r"\btrend\b",
    r"\bbar chart\b",
    r"\bline chart\b",
    r"\bpie chart\b",
]
DEFAULT_TEXT_MODE_PATTERNS = [
    r"\btext only\b",
    r"\bsummary only\b",
    r"\banswer only\b",
    r"\bwithout table\b",
    r"\bno table\b",
    r"\bwithout chart\b",
    r"\bno chart\b",
    r"\bjust explain\b",
]
DEFAULT_REPORT_MODE_PATTERNS = [
    r"\breport\b",
    r"\bsummary report\b",
    r"\bgenerate report\b",
    r"\bshow report\b",
    r"\bcreate report\b",
    r"\bweekly report\b",
    r"\bmonthly report\b",
    r"\bdashboard report\b",
]
DEFAULT_CHART_FOLLOWUP_PATTERNS = [
    r"\bchart\b",
    r"\bgraph\b",
    r"\bplot\b",
    r"\bvisualization\b",
    r"\bvisualisation\b",
    r"\bthis\b",
    r"\bthat\b",
    r"\babove\b",
    r"\bshown\b",
    r"\bgenerated\b",
]
DEFAULT_FOLLOWUP_PATTERNS = [
    r"\bwhat\s+are\s+they\b",
    r"\bwhat\s+are\s+those\b",
    r"\bwhat\s+are\s+these\b",
    r"\bcan\s+you\s+tell\s+me\s+what\s+are\s+they\b",
    r"\bshow them\b",
    r"\bshow those\b",
    r"\bshow records\b",
    r"\bshow packages\b",
    r"\bwhich ones\b",
    r"\blist them\b",
]
DEFAULT_ROW_LIMIT_PATTERNS = [
    r"\b(?:last|latest|recent|top|first)\s+(\d{1,3})(?!\s*(?:day|days|week|weeks|month|months|year|years))\b",
    r"\b(\d{1,3})\s+(?:records|record|rows|row|packages|package|items|item)\b",
]
DEFAULT_CARRIER_FILTERS = {
    "amazon": ["amazon"],
    "usps": ["usps", "postal"],
    "fedex": ["fedex"],
    "ups": ["ups"],
    "dhl": ["dhl"],
}
DEFAULT_EMPTY_DATE_VALUES = ["", "none", "null", "0000-00-00", "0000-00-00 00:00:00"]
DEFAULT_STOP_TERMS = ["the", "a", "an", "shipping", "carrier", "service", "services", "package", "packages", "delivered", "delivery", "today", "yesterday"]
DEFAULT_CONNECTOR_PATTERN = r"\b(?:through|via|to)\s+([a-z0-9\s]{2,40})"
DEFAULT_SENSITIVE_PATTERNS = [
    r"\bpassword\b",
    r"\bpasscode\b",
    r"\bpasswd\b",
    r"\bcredential\b",
    r"\bcredentials\b",
    r"\bsecret\b",
    r"\bpin\b",
    r"\bpin\s*code\b",
    r"\botp\b",
    r"\bapi\s*key\b",
    r"\baccess\s*token\b",
]
DEFAULT_CONTENT_QUERY_TERMS = [
    "delivered",
    "package",
    "packages",
    "tracking",
    "carrier",
    "recipient",
    "status",
    "date",
    "today",
    "latest",
    "last",
    "recent",
    "count",
    "total",
]
DEFAULT_RECIPIENT_WISE_RECIPIENT_TERMS = ["recipient", "recipient name", "each recipient", "per recipient"]
DEFAULT_RECIPIENT_WISE_COUNT_TERMS = ["how many", "count", "total", "number of"]
DEFAULT_RECIPIENT_WISE_GROUP_TERMS = ["each", "per", "individual", "every"]
DEFAULT_RECIPIENT_WISE_DELIVERED_TERMS = ["delivered"]
DEFAULT_RECIPIENT_WISE_TODAY_TERMS = ["today"]
DEFAULT_TOP_RECIPIENT_TERMS = ["top", "maximum", "max", "most", "highest"]
DEFAULT_TOP_RECIPIENT_LOW_TERMS = ["least", "lowest", "fewest", "minimum", "min", "less", "lower"]
DEFAULT_TOP_RECIPIENT_SUBJECT_TERMS = ["recipient", "recipients", "who", "person", "people", "user", "users"]
DEFAULT_TOP_RECIPIENT_PACKAGE_TERMS = ["package", "packages"]
DEFAULT_TOP_RECIPIENT_DEFAULT_LIMIT = 10
DEFAULT_SINGLE_COLUMN_TEXT_MAX_VALUES = 25
DEFAULT_RECIPIENT_STATUS_CODE_MAP = {
    "active": [1],
    "inactive": [0],
    "future": [2],
}
DEFAULT_RECIPIENT_TEMPLATE_KEYWORDS = {
    "recipient": [
        "recipient",
        "recipients",
        "account holder",
        "resident",
        "user",
        "users",
        "member",
        "members",
    ],
    "package": ["package", "packages", "tracking", "carrier"],
    "contact": ["email", "cell", "phone", "mobile", "contact"],
    "count": ["count", "total", "how many", "number of"],
    "listing": ["who", "which", "list", "show", "all", "give"],
}
DEFAULT_RECIPIENT_STATUS_TERMS = {
    "active": ["active", "enabled", "working", "currently working"],
    "inactive": [
        "inactive",
        "disabled",
        "deactivated",
        "not working",
        "non working",
        "not active",
    ],
    "future": ["future", "upcoming", "pending", "will be added", "to be added"],
}
DEFAULT_RECIPIENT_STATUS_PRIORITY = ["inactive", "active", "future"]
DEFAULT_STRICT_DETAIL_ENABLE_PATTERNS = [
    r"\bonly\b.*\b(data|details|columns|fields|result|results)\b",
    r"\bonly\s+show\b",
    r"\bjust\s+show\b",
    r"\bonly\s+particular\b",
    r"\bonly\s+required\b",
    r"\bonly\s+that\b",
    r"\bno\s+extra\b",
]
DEFAULT_STRICT_DETAIL_DISABLE_PATTERNS = [
    r"\bextra\s+(info|information|details)\b",
    r"\bmore\s+details\b",
    r"\bfull\s+details\b",
    r"\ball\s+details\b",
    r"\bcomplete\s+details\b",
]


def _load_string_list_env(name, default_list):
    """Load list[str] from JSON env var with fallback."""
    values = _load_json_env(name, default_list)
    if not isinstance(values, list):
        return list(default_list)
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    return cleaned or list(default_list)


def _load_dict_list_env(name, default_map):
    """Load dict[str, list[str]] from JSON env var with fallback."""
    values = _load_json_env(name, default_map)
    if not isinstance(values, dict):
        return dict(default_map)

    normalized = {}
    for key, aliases in values.items():
        key_text = str(key).strip().lower()
        if not key_text:
            continue
        if isinstance(aliases, list):
            alias_values = [str(alias).strip().lower() for alias in aliases if str(alias).strip()]
        else:
            alias_values = [str(aliases).strip().lower()] if str(aliases).strip() else []
        if alias_values:
            normalized[key_text] = alias_values

    return normalized or dict(default_map)


def _load_status_code_map_env(name, default_map):
    """Load recipient status-code mapping from env with safe fallback."""
    values = _load_json_env(name, default_map)
    if not isinstance(values, dict):
        return dict(default_map)

    normalized = {}
    for key, raw_codes in values.items():
        key_text = str(key).strip().lower()
        if not key_text:
            continue

        if isinstance(raw_codes, list):
            code_values = raw_codes
        else:
            code_values = [raw_codes]

        parsed_codes = []
        for code in code_values:
            text_code = str(code).strip()
            if not text_code:
                continue
            if re.fullmatch(r"\d+", text_code):
                parsed_codes.append(int(text_code))
            else:
                parsed_codes.append(text_code.lower())

        if parsed_codes:
            normalized[key_text] = parsed_codes

    return normalized or dict(default_map)


def _load_json_env(name, default_value):
    """Load JSON config from environment with safe fallback."""
    raw_value = str(os.getenv(name, "") or "").strip()
    if not raw_value:
        return default_value
    try:
        return json.loads(raw_value)
    except ValueError:
        logger.warning("Invalid JSON in %s; using default config.", name)
        return default_value


def get_dynamic_greeting_replies():
    """Get greeting replies from env override or default set."""
    env_replies = _load_json_env("CHATBOT_GREETING_REPLIES", [])
    if isinstance(env_replies, list):
        cleaned = [str(item).strip() for item in env_replies if str(item).strip()]
        if cleaned:
            return cleaned
    return list(DEFAULT_GREETING_REPLIES)


def _column_tokens(columns):
    """Build vocabulary tokens from schema column names."""
    tokens = set()
    for column in columns:
        normalized = str(column).strip().lower()
        if not normalized:
            continue
        tokens.add(normalized)
        tokens.add(normalized.replace("_", " "))
        for part in re.split(r"[_\W]+", normalized):
            if len(part) >= 3:
                tokens.add(part)
    return tokens


DEFAULT_TYPO_MAP = {
    "god": "good",
    "mornng": "morning",
    "morng": "morning",
    "evning": "evening",
    "helo": "hello",
    "pakage": "package",
    "pakages": "packages",
    "pakge": "package",
    "packge": "package",
    "packges": "packages",
    "traking": "tracking",
    "trcking": "tracking",
    "trakcing": "tracking",
    "trak": "track",
    "carier": "carrier",
    "staus": "status",
    "acount": "account",
    "accound": "account",
    "accunt": "account",
    "idd": "id",
    "numbr": "number",
    "nummber": "number",
    "deliverd": "delivered",
    "deleivered": "delivered",
    "delievered": "delivered",
    "delivred": "delivered",
    "delvery": "delivery",
    "amazn": "amazon",
    "amzon": "amazon",
    "usp": "usps",
    "uspost": "usps",
    "portal": "postal",
    "postel": "postal",
    "servce": "service",
    "sevice": "service",
    "liust": "list",
    "lst": "list",
    "tabel": "table",
    "tabl": "table",
    "datta": "data",
    "recods": "records",
    "shippng": "shipping",
    "shiping": "shipping",
    "querry": "query",
    "quaries": "queries",
    "recpient": "recipient",
}

DEFAULT_BASE_VOCAB = {
    "good",
    "morning",
    "evening",
    "afternoon",
    "night",
    "nite",
    "hello",
    "hi",
    "hey",
    "help",
    "query",
    "queries",
    "account",
    "id",
    "package",
    "packages",
    "tracking",
    "track",
    "carrier",
    "status",
    "count",
    "total",
    "number",
    "shipment",
    "delivered",
    "delivery",
    "latest",
    "recent",
    "date",
    "today",
    "table",
    "list",
    "data",
    "records",
    "record",
    "row",
    "rows",
    "show",
    "amazon",
    "usps",
    "postal",
    "fedex",
    "ups",
    "dhl",
}

SHORT_TOKEN_FUZZY_BLOCKLIST = {
    "and",
    "are",
    "can",
    "for",
    "from",
    "has",
    "have",
    "how",
    "its",
    "not",
    "that",
    "the",
    "this",
    "was",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "with",
    "you",
    "your",
}


def build_typo_map():
    """Build typo map from defaults + optional env overrides."""
    merged = dict(DEFAULT_TYPO_MAP)
    extra = _load_json_env("CHATBOT_TYPO_MAP", {})
    if isinstance(extra, dict):
        for key, value in extra.items():
            key_text = str(key).strip().lower()
            value_text = str(value).strip().lower()
            if key_text and value_text:
                merged[key_text] = value_text
    return merged


def build_vocabulary(columns=None):
    """Build normalization vocabulary from defaults + schema + env extras."""
    resolved_columns = columns or ALL_SUPPORTED_COLUMNS
    vocab = set(DEFAULT_BASE_VOCAB)
    vocab.update(_column_tokens(resolved_columns))
    extra = _load_json_env("CHATBOT_VOCAB_EXTRA", [])
    if isinstance(extra, list):
        for item in extra:
            token = str(item).strip().lower()
            if token:
                vocab.add(token)
    return vocab


def build_field_alias_map(columns):
    """Build dynamic field aliases from schema columns and env overrides."""
    alias_map = {}
    for column in columns:
        column_name = str(column).strip().lower()
        if not column_name:
            continue

        aliases = {column_name, column_name.replace("_", " ")}
        parts = column_name.split("_")
        if len(parts) >= 2 and parts[-1] == "id":
            aliases.add(" ".join(parts[:-1]))
        if column_name.startswith("date_"):
            aliases.add(column_name.replace("date_", "") + " date")

        alias_map[column_name] = aliases

    extra_aliases = _load_json_env("CHATBOT_FIELD_ALIASES", {})
    if isinstance(extra_aliases, dict):
        for column_name, aliases in extra_aliases.items():
            normalized_col = str(column_name).strip().lower()
            if normalized_col not in alias_map:
                continue
            if isinstance(aliases, list):
                for alias in aliases:
                    alias_text = str(alias).strip().lower()
                    if alias_text:
                        alias_map[normalized_col].add(alias_text)
    return alias_map


def build_db_intent_terms():
    """Build DB intent terms dynamically from schema and env extras."""
    terms = set(_column_tokens(ALL_SUPPORTED_COLUMNS))
    # Include supported table-name tokens so module/table mentions such as
    # "tv sessions" are recognized as DB intent even when data-keywords are typoed.
    terms.update(_column_tokens(SUPPORTED_QUERY_TABLES))
    terms.update({"table", "list", "data", "record", "records", "row", "rows", "show"})
    extra_terms = _load_json_env("CHATBOT_DB_INTENT_TERMS", [])
    if isinstance(extra_terms, list):
        for item in extra_terms:
            token = str(item).strip().lower()
            if token:
                terms.add(token)
    return terms


DEFAULT_SENSITIVE_COLUMN_KEYWORDS = [
    "password",
    "passcode",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "auth",
    "otp",
    "pin",
    "ssn",
    "social",
    "birth_date",
    "dob",
    "email",
    "cellphone",
    "phone",
    "mobile",
    "address",
    "card",
    "cc_",
    "billing",
    "payment",
]


SENSITIVE_COLUMN_KEYWORDS = tuple(
    _load_string_list_env("CHATBOT_SENSITIVE_COLUMN_KEYWORDS", DEFAULT_SENSITIVE_COLUMN_KEYWORDS)
)


def _is_sensitive_column_name(column_name):
    """Return True when column name should be masked in chatbot output."""
    normalized = str(column_name or "").strip().lower()
    if not normalized:
        return False

    # Avoid substring false-positives on short keywords (for example, "pin"
    # unintentionally matching "shipping_carrier").
    tokens = {token for token in re.split(r"[^a-z0-9]+", normalized) if token}
    for keyword in SENSITIVE_COLUMN_KEYWORDS:
        key = str(keyword or "").strip().lower()
        if not key:
            continue

        # Prefix-style keywords such as "cc_" should match token prefixes.
        if key.endswith("_"):
            prefix = key.rstrip("_")
            if any(token.startswith(prefix) for token in tokens):
                return True
            continue

        # Short keywords must match whole tokens only.
        if len(key) <= 3:
            if key in tokens:
                return True
            continue

        if key in normalized:
            return True

    return False


def _mask_sensitive_value(column_name, value):
    """Mask sensitive values while preserving non-sensitive values untouched."""
    if value is None:
        return None
    if not _is_sensitive_column_name(column_name):
        return value

    key_text = str(column_name).strip().lower()
    text_value = str(value).strip()
    if not text_value:
        return value

    if "email" in key_text and "@" in text_value:
        local_part, domain_part = text_value.split("@", 1)
        if not local_part:
            return f"***@{domain_part}"
        if len(local_part) == 1:
            return f"{local_part}***@{domain_part}"
        return f"{local_part[0]}***{local_part[-1]}@{domain_part}"

    if any(token in key_text for token in ("cell", "phone", "mobile", "pin", "otp", "card", "cc_")):
        digits_only = re.sub(r"\D", "", text_value)
        if len(digits_only) >= 4:
            return f"***{digits_only[-4:]}"
        return "***"

    return "REDACTED"


def _mask_row_for_display(row):
    """Apply sensitive-field masking to one row dictionary."""
    if not isinstance(row, dict):
        return row
    masked = {}
    for key, value in row.items():
        masked[str(key)] = _mask_sensitive_value(key, value)
    return masked


def _mask_rows_for_display(rows):
    """Apply sensitive-field masking to a list of row dictionaries."""
    if not isinstance(rows, list):
        return rows
    return [_mask_row_for_display(row) if isinstance(row, dict) else row for row in rows]


def _get_table_columns_from_schema(schema_map, table_name, fallback_columns):
    """Extract live table columns from schema map when available."""
    if not isinstance(schema_map, dict):
        return fallback_columns
    lookup_name = str(table_name).strip().lower()
    for current_table_name, columns in schema_map.items():
        if str(current_table_name).strip().lower() == lookup_name and isinstance(columns, list) and columns:
            return [str(column).strip() for column in columns if str(column).strip()]
    return fallback_columns


def _get_supported_columns_from_schema(schema_map, supported_tables=None):
    """Return schema columns for all supported tables with sane fallbacks."""
    resolved_tables = supported_tables or _get_runtime_supported_tables(schema_map)
    table_columns = {}
    for table_name in resolved_tables:
        fallback_columns = SUPPORTED_TABLE_DEFAULT_COLUMNS.get(table_name, [])
        table_columns[table_name] = _get_table_columns_from_schema(
            schema_map,
            table_name,
            fallback_columns,
        )
    return table_columns


def refresh_dynamic_language_resources(schema_map=None):
    """Refresh vocabulary/aliases/intent terms from live schema and env config."""
    global NORMALIZATION_VOCAB
    global NORMALIZATION_VOCAB_SET
    global NORMALIZATION_VOCAB_VERSION
    global FIELD_ALIAS_MAP
    global DB_INTENT_TERMS

    runtime_tables = _get_runtime_supported_tables(schema_map)
    table_columns = _get_supported_columns_from_schema(schema_map, supported_tables=runtime_tables)
    merged_columns = []
    for table_name in runtime_tables:
        for column in table_columns.get(table_name, []):
            if column not in merged_columns:
                merged_columns.append(column)

    if not merged_columns:
        merged_columns = list(ALL_SUPPORTED_COLUMNS)

    NORMALIZATION_VOCAB = build_vocabulary(merged_columns)
    NORMALIZATION_VOCAB_SET = set(NORMALIZATION_VOCAB)
    NORMALIZATION_VOCAB_VERSION += 1
    _normalize_intent_text_cached.cache_clear()
    _fuzzy_cached_token_match.cache_clear()
    FIELD_ALIAS_MAP = build_field_alias_map(merged_columns)
    DB_INTENT_TERMS = set(_column_tokens(merged_columns))
    DB_INTENT_TERMS.update({"table", "list", "data", "record", "records", "row", "rows", "show"})
    extra_terms = _load_json_env("CHATBOT_DB_INTENT_TERMS", [])
    if isinstance(extra_terms, list):
        for item in extra_terms:
            token = str(item).strip().lower()
            if token:
                DB_INTENT_TERMS.add(token)


TYPO_MAP = build_typo_map()
NORMALIZATION_VOCAB = build_vocabulary()
NORMALIZATION_VOCAB_SET = set(NORMALIZATION_VOCAB)
NORMALIZATION_VOCAB_VERSION = 1
FIELD_ALIAS_MAP = build_field_alias_map(ALL_SUPPORTED_COLUMNS)
DB_INTENT_TERMS = build_db_intent_terms()
TABLE_VIEW_TERMS = set(_load_json_env("CHATBOT_TABLE_VIEW_TERMS", ["table", "list", "tabular", "rows", "records"]))
LATEST_DATE_TERMS = set(_load_json_env("CHATBOT_LATEST_DATE_TERMS", ["last", "latest", "recent", "most recent", "newest"]))
GREETING_PATTERNS = _load_string_list_env("CHATBOT_GREETING_PATTERNS", DEFAULT_GREETING_PATTERNS)
HELP_TERMS = set(_load_string_list_env("CHATBOT_HELP_TERMS", DEFAULT_HELP_TERMS))
HELP_SUBJECT_TERMS = set(_load_string_list_env("CHATBOT_HELP_SUBJECT_TERMS", DEFAULT_HELP_SUBJECT_TERMS))
HELP_ACTION_TERMS = set(_load_string_list_env("CHATBOT_HELP_ACTION_TERMS", DEFAULT_HELP_ACTION_TERMS))
HELP_CONTEXT_TERMS = set(_load_string_list_env("CHATBOT_HELP_CONTEXT_TERMS", DEFAULT_HELP_CONTEXT_TERMS))
DATE_TOKEN_TERMS = set(_load_string_list_env("CHATBOT_DATE_TOKEN_TERMS", DEFAULT_DATE_TOKEN_TERMS))
PEAK_TERMS = set(_load_string_list_env("CHATBOT_PEAK_TERMS", DEFAULT_PEAK_TERMS))
DISPLAY_MODE_PATTERNS = _load_string_list_env("CHATBOT_DISPLAY_MODE_PATTERNS", DEFAULT_DISPLAY_MODE_PATTERNS)
CHART_MODE_PATTERNS = _load_string_list_env("CHATBOT_CHART_MODE_PATTERNS", DEFAULT_CHART_MODE_PATTERNS)
TEXT_MODE_PATTERNS = _load_string_list_env("CHATBOT_TEXT_MODE_PATTERNS", DEFAULT_TEXT_MODE_PATTERNS)
REPORT_MODE_PATTERNS = _load_string_list_env("CHATBOT_REPORT_MODE_PATTERNS", DEFAULT_REPORT_MODE_PATTERNS)
CHART_FOLLOWUP_PATTERNS = _load_string_list_env("CHATBOT_CHART_FOLLOWUP_PATTERNS", DEFAULT_CHART_FOLLOWUP_PATTERNS)
FOLLOWUP_RECORD_PATTERNS = _load_string_list_env("CHATBOT_FOLLOWUP_PATTERNS", DEFAULT_FOLLOWUP_PATTERNS)
ROW_LIMIT_PATTERNS = _load_string_list_env("CHATBOT_ROW_LIMIT_PATTERNS", DEFAULT_ROW_LIMIT_PATTERNS)
CARRIER_FILTERS = _load_dict_list_env("CHATBOT_CARRIER_FILTERS", DEFAULT_CARRIER_FILTERS)
EMPTY_DATE_VALUES = set(_load_string_list_env("CHATBOT_EMPTY_DATE_VALUES", DEFAULT_EMPTY_DATE_VALUES))
PHRASE_STOP_TERMS = set(_load_string_list_env("CHATBOT_STOP_TERMS", DEFAULT_STOP_TERMS))
PHRASE_CONNECTOR_PATTERN = os.getenv("CHATBOT_CONNECTOR_PATTERN", DEFAULT_CONNECTOR_PATTERN)
CONTENT_QUERY_TERMS = set(_load_string_list_env("CHATBOT_CONTENT_QUERY_TERMS", DEFAULT_CONTENT_QUERY_TERMS))
SENSITIVE_QUERY_PATTERNS = _load_string_list_env("CHATBOT_SENSITIVE_PATTERNS", DEFAULT_SENSITIVE_PATTERNS)
RECIPIENT_WISE_RECIPIENT_TERMS = set(_load_string_list_env("CHATBOT_RECIPIENT_WISE_RECIPIENT_TERMS", DEFAULT_RECIPIENT_WISE_RECIPIENT_TERMS))
RECIPIENT_WISE_COUNT_TERMS = set(_load_string_list_env("CHATBOT_RECIPIENT_WISE_COUNT_TERMS", DEFAULT_RECIPIENT_WISE_COUNT_TERMS))
RECIPIENT_WISE_GROUP_TERMS = set(_load_string_list_env("CHATBOT_RECIPIENT_WISE_GROUP_TERMS", DEFAULT_RECIPIENT_WISE_GROUP_TERMS))
RECIPIENT_WISE_DELIVERED_TERMS = set(_load_string_list_env("CHATBOT_RECIPIENT_WISE_DELIVERED_TERMS", DEFAULT_RECIPIENT_WISE_DELIVERED_TERMS))
RECIPIENT_WISE_TODAY_TERMS = set(_load_string_list_env("CHATBOT_RECIPIENT_WISE_TODAY_TERMS", DEFAULT_RECIPIENT_WISE_TODAY_TERMS))
TOP_RECIPIENT_TERMS = set(_load_string_list_env("CHATBOT_TOP_RECIPIENT_TERMS", DEFAULT_TOP_RECIPIENT_TERMS))
TOP_RECIPIENT_LOW_TERMS = set(_load_string_list_env("CHATBOT_TOP_RECIPIENT_LOW_TERMS", DEFAULT_TOP_RECIPIENT_LOW_TERMS))
TOP_RECIPIENT_SUBJECT_TERMS = set(_load_string_list_env("CHATBOT_TOP_RECIPIENT_SUBJECT_TERMS", DEFAULT_TOP_RECIPIENT_SUBJECT_TERMS))
TOP_RECIPIENT_PACKAGE_TERMS = set(_load_string_list_env("CHATBOT_TOP_RECIPIENT_PACKAGE_TERMS", DEFAULT_TOP_RECIPIENT_PACKAGE_TERMS))
COUNT_INTENT_TERMS = ("how many", "count", "total", "number of")
CARRIER_SUBJECT_TERMS = ("carrier", "shipping carrier", "service", "postal")
CONNECT_MESSAGE_CONTEXT_TERMS = ("message", "messages", "mailqueue", "email", "text", "sms", "sent")
CONNECT_QUEUE_HINT_TERMS = ("mailqueue", "mail queue", "queue")
CONNECT_SENT_TERMS = ("sent", "send", "sending")
TOP_RECIPIENT_DEFAULT_LIMIT = max(1, min(int(os.getenv("CHATBOT_TOP_RECIPIENT_DEFAULT_LIMIT", str(DEFAULT_TOP_RECIPIENT_DEFAULT_LIMIT))), 100))
CHART_MAX_POINTS = max(5, min(int(os.getenv("CHATBOT_CHART_MAX_POINTS", "120")), 500))
CHART_MAX_CATEGORIES = max(3, min(int(os.getenv("CHATBOT_CHART_MAX_CATEGORIES", "12")), 50))
CHART_MAX_PIE_CATEGORIES = max(3, min(int(os.getenv("CHATBOT_CHART_MAX_PIE_CATEGORIES", "20")), 50))
CHART_MAX_DATA_LABELS = max(5, min(int(os.getenv("CHATBOT_CHART_MAX_DATA_LABELS", "60")), 500))
REPORT_TABLE_DEFAULT_LIMIT = max(10, min(int(os.getenv("CHATBOT_REPORT_TABLE_DEFAULT_LIMIT", "100")), 500))
STRICT_DETAIL_ENABLE_PATTERNS = _load_string_list_env("CHATBOT_STRICT_DETAIL_ENABLE_PATTERNS", DEFAULT_STRICT_DETAIL_ENABLE_PATTERNS)
STRICT_DETAIL_DISABLE_PATTERNS = _load_string_list_env("CHATBOT_STRICT_DETAIL_DISABLE_PATTERNS", DEFAULT_STRICT_DETAIL_DISABLE_PATTERNS)
RECIPIENT_WISE_COUNT_ALIAS = str(os.getenv("CHATBOT_RECIPIENT_WISE_COUNT_ALIAS", "package_count") or "package_count").strip().lower()
RECIPIENT_WISE_COUNT_METRIC_COLUMN = str(os.getenv("CHATBOT_RECIPIENT_WISE_COUNT_METRIC_COLUMN", "*") or "*").strip().lower()
SINGLE_COLUMN_TEXT_MAX_VALUES = max(1, min(int(os.getenv("CHATBOT_SINGLE_COLUMN_TEXT_MAX_VALUES", str(DEFAULT_SINGLE_COLUMN_TEXT_MAX_VALUES))), 200))
RECIPIENT_STATUS_CODE_MAP = _load_status_code_map_env(
    "CHATBOT_RECIPIENT_STATUS_CODE_MAP",
    DEFAULT_RECIPIENT_STATUS_CODE_MAP,
)
RECIPIENT_TEMPLATE_KEYWORDS = _load_dict_list_env(
    "CHATBOT_RECIPIENT_TEMPLATE_KEYWORDS",
    DEFAULT_RECIPIENT_TEMPLATE_KEYWORDS,
)
RECIPIENT_STATUS_TERMS = _load_dict_list_env(
    "CHATBOT_RECIPIENT_STATUS_TERMS",
    DEFAULT_RECIPIENT_STATUS_TERMS,
)
RECIPIENT_STATUS_PRIORITY = [
    str(item).strip().lower()
    for item in _load_string_list_env("CHATBOT_RECIPIENT_STATUS_PRIORITY", DEFAULT_RECIPIENT_STATUS_PRIORITY)
    if str(item).strip()
]


def _contains_any_term(text, terms):
    """Return True if any configured term appears in normalized text."""
    if not text or not terms:
        return False

    for term in terms:
        normalized_term = str(term).strip().lower()
        if not normalized_term:
            continue
        if " " in normalized_term:
            if normalized_term in text:
                return True
        else:
            if re.search(rf"\b{re.escape(normalized_term)}\b", text):
                return True
    return False


def _has_count_intent(text):
    """Return True when the query asks for a count/total quantity."""
    return _contains_any_term(text, COUNT_INTENT_TERMS)


def _has_carrier_subject(text):
    """Return True when query mentions carrier-related subject terms."""
    return _contains_any_term(text, CARRIER_SUBJECT_TERMS)


def log_chat_interaction(account_id, user_query, generated_sql, status, rows_count, answer):
    """Centralized SQL chat logging for traceability."""
    _append_sql_table_row(account_id, user_query, generated_sql, status, rows_count, answer)


def log_routing_debug(account_id, user_query, intent, model_name, stage, extra=""):
    """Emit per-request routing diagnostics to terminal when enabled."""
    if not ROUTING_DEBUG:
        return

    safe_query = str(user_query or "").replace("\n", " ").strip()
    if len(safe_query) > 160:
        safe_query = safe_query[:157] + "..."
    logger.info(
        "[ROUTER] account_id=%s | stage=%s | intent=%s | model=%s | query=%s | extra=%s",
        account_id,
        stage,
        intent,
        model_name,
        safe_query,
        extra,
    )


def call_model(api_url, model_name, prompt, timeout_seconds):
    """Call an Ollama-compatible API model and return text output."""
    try:
        response = requests.post(
            api_url,
            json={"model": model_name, "prompt": prompt, "stream": False},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        response_json = response.json()
        return str(response_json.get("response", "")).strip()
    except requests.RequestException as error:
        logger.error("Model request failed (model=%s): %s", model_name, error)
        return ""
    except ValueError as error:
        logger.error("Model response parse failed (model=%s): %s", model_name, error)
        return ""


def call_qwen(prompt, timeout_seconds=None):
    """Call Qwen model for generic/chat intents."""
    resolved_timeout = QWEN_TIMEOUT if timeout_seconds is None else max(1, int(timeout_seconds))
    return call_model(QWEN_API_URL, QWEN_MODEL, prompt, resolved_timeout)


def _build_sql_gen_cache_key(account_id, user_query, supported_tables):
    """Build stable cache key for SQL generation by account/query/table-scope."""
    normalized_query = normalize_intent_text(user_query)
    normalized_tables = tuple(str(table).strip().lower() for table in (supported_tables or ()))
    return f"{str(account_id).strip()}|{normalized_query}|{','.join(normalized_tables)}"


def _get_cached_sql_generation(account_id, user_query, supported_tables):
    """Return cached generated SQL when entry is fresh."""
    if not SQL_GEN_CACHE_ENABLED:
        return None

    cache_key = _build_sql_gen_cache_key(account_id, user_query, supported_tables)
    now = time.time()
    with SQL_GEN_CACHE_LOCK:
        entry = SQL_GEN_CACHE.get(cache_key)
        if not entry:
            return None
        if now - float(entry.get("created_at", 0.0)) > SQL_GEN_CACHE_TTL_S:
            SQL_GEN_CACHE.pop(cache_key, None)
            return None
        return str(entry.get("sql", "") or "")


def _set_cached_sql_generation(account_id, user_query, supported_tables, generated_sql):
    """Store generated SQL in bounded TTL cache."""
    if not SQL_GEN_CACHE_ENABLED:
        return

    sql_text = str(generated_sql or "").strip()
    if not sql_text:
        return

    cache_key = _build_sql_gen_cache_key(account_id, user_query, supported_tables)
    now = time.time()
    with SQL_GEN_CACHE_LOCK:
        SQL_GEN_CACHE[cache_key] = {"sql": sql_text, "created_at": now}
        if len(SQL_GEN_CACHE) > SQL_GEN_CACHE_MAX_ITEMS:
            ordered_keys = sorted(SQL_GEN_CACHE.keys(), key=lambda key: SQL_GEN_CACHE[key].get("created_at", 0.0))
            remove_count = len(SQL_GEN_CACHE) - SQL_GEN_CACHE_MAX_ITEMS
            for stale_key in ordered_keys[:remove_count]:
                SQL_GEN_CACHE.pop(stale_key, None)


def _build_chart_payload_cache_key(account_id, user_query, generated_sql):
    """Build stable key for chart payload cache based on account/query/sql hash."""
    account_part = str(account_id or "").strip().lower()
    query_part = normalize_intent_text(user_query)
    sql_part = normalize_generated_sql_for_log(generated_sql)
    sql_hash = hashlib.sha256(sql_part.encode("utf-8")).hexdigest() if sql_part else "no_sql"
    return f"{account_part}|{query_part}|{sql_hash}"


def _get_cached_chart_payload(account_id, user_query, generated_sql):
    """Return cached chart payload when still fresh."""
    if not CHART_PAYLOAD_CACHE_ENABLED:
        return None

    cache_key = _build_chart_payload_cache_key(account_id, user_query, generated_sql)
    now = time.time()
    with CHART_PAYLOAD_CACHE_LOCK:
        entry = CHART_PAYLOAD_CACHE.get(cache_key)
        if not entry:
            return None
        if now - float(entry.get("created_at", 0.0)) > CHART_PAYLOAD_CACHE_TTL_S:
            CHART_PAYLOAD_CACHE.pop(cache_key, None)
            return None
        payload = entry.get("payload")
        return payload if isinstance(payload, dict) else None


def _set_cached_chart_payload(account_id, user_query, generated_sql, payload):
    """Store chart payload in bounded TTL cache."""
    if not CHART_PAYLOAD_CACHE_ENABLED or not isinstance(payload, dict) or not payload:
        return

    cache_key = _build_chart_payload_cache_key(account_id, user_query, generated_sql)
    now = time.time()
    with CHART_PAYLOAD_CACHE_LOCK:
        CHART_PAYLOAD_CACHE[cache_key] = {"payload": payload, "created_at": now}
        if len(CHART_PAYLOAD_CACHE) > CHART_PAYLOAD_CACHE_MAX_ITEMS:
            ordered_keys = sorted(
                CHART_PAYLOAD_CACHE.keys(),
                key=lambda key: CHART_PAYLOAD_CACHE[key].get("created_at", 0.0),
            )
            remove_count = len(CHART_PAYLOAD_CACHE) - CHART_PAYLOAD_CACHE_MAX_ITEMS
            for stale_key in ordered_keys[:remove_count]:
                CHART_PAYLOAD_CACHE.pop(stale_key, None)


def _build_query_response_cache_key(account_id, user_query, response_detail_mode, account_data_version):
    """Build stable key for full chatbot responses."""
    account_part = str(account_id or "").strip().lower()
    query_part = normalize_intent_text(user_query)
    detail_part = str(response_detail_mode or "rich").strip().lower()
    data_part = str(account_data_version or "0").strip().lower()
    return f"{account_part}|{detail_part}|{data_part}|{query_part}"


def _get_cached_query_response(cache_key):
    """Return cached response payload when still fresh."""
    if not QUERY_RESPONSE_CACHE_ENABLED or not cache_key:
        return None

    now = time.time()
    with QUERY_RESPONSE_CACHE_LOCK:
        entry = QUERY_RESPONSE_CACHE.get(cache_key)
        if not entry:
            return None
        if now - float(entry.get("created_at", 0.0)) > QUERY_RESPONSE_CACHE_TTL_S:
            QUERY_RESPONSE_CACHE.pop(cache_key, None)
            return None
        payload = entry.get("payload")
        return payload if isinstance(payload, dict) else None


def _set_cached_query_response(cache_key, payload):
    """Store full chatbot response payload in bounded TTL cache."""
    if not QUERY_RESPONSE_CACHE_ENABLED or not cache_key or not isinstance(payload, dict) or not payload:
        return

    now = time.time()
    with QUERY_RESPONSE_CACHE_LOCK:
        QUERY_RESPONSE_CACHE[cache_key] = {"payload": payload, "created_at": now}
        if len(QUERY_RESPONSE_CACHE) > QUERY_RESPONSE_CACHE_MAX_ITEMS:
            ordered_keys = sorted(
                QUERY_RESPONSE_CACHE.keys(),
                key=lambda key: QUERY_RESPONSE_CACHE[key].get("created_at", 0.0),
            )
            remove_count = len(QUERY_RESPONSE_CACHE) - QUERY_RESPONSE_CACHE_MAX_ITEMS
            for stale_key in ordered_keys[:remove_count]:
                QUERY_RESPONSE_CACHE.pop(stale_key, None)


def should_skip_query_response_cache(user_query, pending_action=None):
    """Skip global response cache for context-dependent follow-up questions."""
    if pending_action:
        return True

    if is_chart_followup_question(user_query):
        return True
    if is_format_only_followup_request(user_query):
        return True
    if is_followup_records_request(user_query):
        return True

    return False


def is_aggregate_sql_query(sql_query):
    """Return True when SQL represents aggregated/grouped results unsuitable for row intersection."""
    normalized = normalize_generated_sql_for_log(sql_query).lower()
    if not normalized:
        return False

    if "select *" in normalized:
        return False

    aggregate_tokens = (
        " group by ",
        " having ",
        " count(",
        " sum(",
        " avg(",
        " min(",
        " max(",
        " distinct ",
    )
    return any(token in normalized for token in aggregate_tokens)


def compute_dynamic_sql_timeout(user_query, prompt_tables, explicit_table_mode=False):
    """Compute query-aware SQL generation timeout to reduce tail latency."""
    if explicit_table_mode:
        return EXPLICIT_TABLE_QWEN_TIMEOUT, "explicit_table"

    if not ADAPTIVE_SQL_TIMEOUT_ENABLED:
        return QWEN_TIMEOUT, "adaptive_disabled"

    text = normalize_intent_text(user_query)
    words = re.findall(r"[a-z0-9_]+", text)
    word_count = len(words)
    table_count = len(prompt_tables or ())

    has_rank_limit = extract_ranked_limit_from_raw_query(user_query, max_limit=100) is not None
    has_row_limit = extract_requested_row_limit(user_query, max_limit=100) is not None
    has_agg_intent = any(token in text for token in ("count", "total", "top", "highest", "lowest", "latest"))
    has_complex_intent = any(token in text for token in ("join", "between", "compare", "trend", "percentage", "distribution"))

    if (has_rank_limit or has_row_limit or has_agg_intent) and not has_complex_intent and word_count <= 14 and table_count <= 2:
        return ADAPTIVE_SQL_SIMPLE_TIMEOUT, "adaptive_simple"

    if word_count <= 24 and table_count <= 3:
        return ADAPTIVE_SQL_MEDIUM_TIMEOUT, "adaptive_medium"

    return QWEN_TIMEOUT, "adaptive_full"


def to_json_safe_rows(rows):
    """Convert DB rows to JSON-safe values for API responses."""
    safe_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        masked_row = _mask_row_for_display(row)
        safe_row = {}
        for key, value in masked_row.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                safe_row[key] = value
            else:
                safe_row[key] = str(value)
        safe_rows.append(safe_row)
    return safe_rows


def build_sql_prompt(user_question, account_id, schema_text, allowed_columns_text=None, supported_tables=None):
    """Prompt template to force SQL-only output from Qwen."""
    allowed_columns = allowed_columns_text or ", ".join(ALL_SUPPORTED_COLUMNS)
    supported_tables_text = ", ".join(supported_tables or SUPPORTED_QUERY_TABLES)
    return f"""
You are an expert MySQL query generator.
Generate ONE read-only SQL query for the user question.

Rules:
1) Output SQL only, no explanation, no markdown.
2) Use only SELECT or WITH.
3) Use only these tables: {supported_tables_text}.
3a) If joining track_packages with core_recipients, join on recipient_id and account_id.
3b) Use clear table aliases for joins.
4) Use this schema:
{schema_text}
4a) Authoritative allowed columns:
{allowed_columns}
5) account_id for current user is: {account_id}
6) You MUST include account_id = {account_id} for each referenced table in WHERE clause.
7) Do not use any other table.
8) Add a LIMIT (<= 100) unless aggregation requires no row limit.
9) MySQL dialect only. Do NOT use || for string concatenation.
10) Use CONCAT(...) for string concatenation in MySQL.
11) For "today" filters, use DATE(column) = CURDATE() when column is datetime/date.
12) Output a syntactically valid MySQL query only.
13) Use exact column names from the allowed columns list above.
14) Users may ask in non-technical words and may not know exact column names.
15) Infer user wording semantically from the schema and select the best matching columns.
16) If user phrasing is ambiguous, choose the most likely columns from schema context.
17) Never synthesize identifiers from other columns (example: CONCAT('tracking_no', package_id)).
18) Temporal quantities are NOT row limits. Example: "last 5 months" means a date filter, not LIMIT 5.
19) For relative periods like last/past N day(s)/week(s)/month(s)/year(s), use date filters with DATE_SUB(CURDATE(), INTERVAL N <UNIT>) on a relevant date column.
20) Only use LIMIT when user explicitly asks row count (e.g., "top 10", "latest 5 records") or for a safe default.

User question:
{user_question}
""".strip()


def build_query_rewrite_prompt(user_question):
    """Prompt to normalize arbitrary user phrasing into schema-friendly wording."""
    allowed_columns = ", ".join(ALL_SUPPORTED_COLUMNS)
    supported_tables_text = ", ".join(SUPPORTED_QUERY_TABLES)

    return f"""
You rewrite user questions for SQL intent understanding.

Task:
- Convert messy, indirect, or non-technical user wording into a clear question that maps to supported table fields.
- Preserve the original meaning and filters exactly.
- Keep account context, carrier constraints, date words (today, yesterday, last month), counts, and ranking intent.
- Do not add assumptions.

Rules:
1) Output one short rewritten question only.
2) No SQL, no markdown, no explanations.
3) Keep natural language.
4) Use these authoritative columns when mapping intent:
{allowed_columns}
5) Resolve synonyms and informal wording from context and schema.
6) Keep scope within supported tables: {supported_tables_text}.

Original user question:
{user_question}
""".strip()


def rewrite_user_query_for_sql(user_query):
    """Deterministically normalize noisy text before SQL generation."""
    original = str(user_query or "").strip()
    if not original:
        return ""

    normalized = normalize_intent_text(original)
    if not normalized:
        return original

    if normalized == original.lower():
        return original

    # Keep original wording and append corrected interpretation.
    return f"{original}. Interpreted request: {normalized}"


def build_requested_fields_prompt(user_question):
    """Prompt to infer requested fields from natural-language question."""
    allowed_columns = ", ".join(ALL_SUPPORTED_COLUMNS)

    return f"""
Identify which supported fields the user is asking for.

Rules:
1) Output strict JSON only.
2) JSON format: {{"requested_fields":["field1","field2"]}}
3) Use only allowed columns.
4) If no specific field is requested, return an empty array.

Allowed columns:
{allowed_columns}

User question:
{user_question}
""".strip()


def infer_requested_fields_from_query(user_query):
    """Infer requested fields using local rules (no LLM)."""
    text = normalize_intent_text(user_query)
    if not text:
        return []

    requested = []
    for field, aliases in FIELD_ALIAS_MAP.items():
        if any(alias in text for alias in aliases):
            requested.append(field)

    return requested


def is_open_table_data_request(user_query, requested_fields=None):
    """Return True when user asks for broad table data rather than specific columns."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    broad_terms = (
        "data",
        "details",
        "list",
        "records",
        "record",
        "table",
        "show",
        "all",
    )
    if not any(term in text for term in broad_terms):
        return False

    if _has_count_intent(text) or query_requests_chart_view(text):
        return False

    explicit_column_markers = (
        " column",
        " columns",
        " field",
        " fields",
        " specific",
        " particular",
        " only",
        "tracking number",
        "shipping carrier",
        "recipient name",
    )
    if any(marker in f" {text} " for marker in explicit_column_markers):
        return False

    resolved_fields = requested_fields if isinstance(requested_fields, list) else infer_requested_fields_from_query(user_query)
    normalized_fields = [str(field).strip().lower() for field in resolved_fields if str(field).strip()]
    unique_fields = list(dict.fromkeys(normalized_fields))

    if len(unique_fields) >= 2:
        return False

    if len(unique_fields) == 1:
        only_field = unique_fields[0]
        # If user explicitly says "id", keep column-specific behavior.
        if re.search(r"\bid\b|\bids\b", text):
            return False
        # Single inferred id field often comes from generic words like "packages".
        return only_field in {"package_id", "recipient_id", "account_id"}

    return True


def sql_mentions_requested_fields(sql_query, requested_fields):
    """Check whether generated SQL covers at least one requested field."""
    if not requested_fields:
        return True

    normalized_sql = str(sql_query or "").lower()
    if "select *" in normalized_sql:
        return True

    return any(field in normalized_sql for field in requested_fields)


def build_sql_repair_prompt(user_question, generated_sql, requested_fields, account_id, schema_text, supported_tables=None):
    """Prompt to repair SQL when requested fields are missing in generated SQL."""
    fields_text = ", ".join(requested_fields) if requested_fields else "none"
    supported_tables_text = ", ".join(supported_tables or SUPPORTED_QUERY_TABLES)
    return f"""
You are fixing a MySQL query for supported account tables.

Requirements:
1) Output SQL only.
2) Keep it read-only (SELECT/CTE only).
3) Use only supported tables ({supported_tables_text}) and account_id = {account_id} for each referenced table.
4) Ensure query includes the requested field(s): {fields_text}
5) Preserve user intent and filters.
6) Do not synthesize identifiers from unrelated columns.

Schema:
{schema_text}

User question:
{user_question}

Current SQL:
{generated_sql}
""".strip()


def build_sql_relax_prompt(user_question, generated_sql, account_id, schema_text, supported_tables=None):
    """Prompt to dynamically repair zero-row SQL while preserving user intent."""
    supported_tables_text = ", ".join(supported_tables or SUPPORTED_QUERY_TABLES)
    return f"""
You are fixing a MySQL query for supported account tables.

Goal:
- Current SQL returned no rows.
- Keep user intent the same and improve the SQL so it can return meaningful rows when appropriate.
- Decide dynamically whether constraints are too strict; relax only what is necessary.

Rules:
1) Output SQL only.
2) Read-only query (SELECT/CTE only).
3) Use only supported tables ({supported_tables_text}) and include account_id = {account_id} for each referenced table.
4) Preserve core intent filters (recipient, carrier, status, requested fields).
5) Keep LIMIT <= 100.

Schema:
{schema_text}

User question:
{user_question}

Current SQL:
{generated_sql}
""".strip()


def _sql_has_expected_relative_window(sql_query, user_query):
    """Return True when SQL contains the relative time window requested by the user."""
    window = extract_requested_relative_time_window(user_query)
    if not window:
        return True

    normalized_sql = normalize_generated_sql_for_log(sql_query).lower()
    expected_value = int(window.get("value", 1))
    expected_unit = str(window.get("unit", "DAY")).lower()
    return re.search(
        rf"date_sub\s*\(\s*curdate\s*\(\s*\)\s*,\s*interval\s+{expected_value}\s+{expected_unit}\s*\)",
        normalized_sql,
    ) is not None


def detect_sql_intent_mismatch_reason(user_query, sql_query):
    """Detect key mismatches between user intent and generated SQL."""
    normalized_sql = normalize_generated_sql_for_log(sql_query).lower()
    if not normalized_sql:
        return "empty_sql"

    window = extract_requested_relative_time_window(user_query)
    explicit_row_limit = extract_requested_row_limit(user_query, max_limit=100)

    if window:
        if not _sql_has_expected_relative_window(normalized_sql, user_query):
            return "missing_relative_time_window"

        if explicit_row_limit is None:
            suspicious_limit = re.search(r"\blimit\s+(\d{1,3})\b", normalized_sql)
            if suspicious_limit and int(suspicious_limit.group(1)) == int(window.get("value", 0)):
                return "time_quantity_used_as_row_limit"

    return ""


def build_sql_intent_alignment_prompt(user_question, generated_sql, account_id, schema_text, supported_tables=None):
    """Prompt to realign SQL with the original natural-language intent."""
    supported_tables_text = ", ".join(supported_tables or SUPPORTED_QUERY_TABLES)
    return f"""
You are correcting a MySQL query so it exactly matches user intent.

Rules:
1) Output SQL only.
2) Read-only query (SELECT/CTE only).
3) Use only supported tables ({supported_tables_text}) and include account_id = {account_id} for each referenced table.
4) Preserve the user's original meaning exactly.
5) Do NOT convert time quantities into row limits.
6) For phrases like last/past N day(s)/week(s)/month(s)/year(s), use DATE_SUB(CURDATE(), INTERVAL N <UNIT>) on relevant date fields.
7) Use LIMIT only if user explicitly requested row count (top/latest N records) or safe default.

Schema:
{schema_text}

Original user question:
{user_question}

Current SQL to correct:
{generated_sql}
""".strip()


def build_answer_prompt(user_question, sql_query, rows):
    """Prompt template to generate concise chatbot answer from query results."""
    sample_rows = to_json_safe_rows(rows[:30])
    allowed_columns = ", ".join(ALL_SUPPORTED_COLUMNS)
    lowered_question = str(user_question or "").lower()
    wants_date_format = any(
        token in lowered_question
        for token in (
            "date",
            "dates",
            "day",
            "delivered on",
            "received on",
            "pickup date",
            "when",
        )
    )
    date_rule = ""
    if wants_date_format:
        date_rule = (
            "If the user asks for date values (date received, delivered date, pickup date, dates list), "
            "format each date as MM/dd/yyyy. "
            "For datetime values, ignore the time portion and output only MM/dd/yyyy."
        )

    return f"""
You are an organizational data assistant.
Answer using only the SQL output rows.
Tone: professional, clear, and concise.
Respond in natural language only.
Do not return JSON, Python lists, or dictionary-like output.
If rows are empty, clearly say no matching organizational data was found.
If the user asks for a count/total, compute it from the provided rows.
If the user asks for exact fields (tracking number, recipient name, carrier, status, package id), return those exact values from the rows.
If multiple values exist, provide a concise comma-separated list and mention the total number of values.
Do not invent or assume missing values.
{date_rule}
Use exact field names from this track_packages column list when interpreting user questions:
{allowed_columns}

Question:
{user_question}

SQL:
{sql_query}

Rows:
{sample_rows}
""".strip()


def build_greeting_prompt(user_message):
    """Prompt for dynamic greeting replies."""
    return f"""
You are a professional support chatbot.
The user sent a greeting.
Reply with one short, friendly natural-language greeting.
Mention you can help with account package data questions.
Do not use JSON, lists, or markdown.

User message:
{user_message}
""".strip()


def build_out_of_scope_prompt(user_message):
    """Prompt for dynamic out-of-scope responses."""
    return f"""
You are a professional support chatbot focused only on account package data.
The user's message is out of your supported data scope.
Reply with one short, polite natural-language response saying you cannot answer because sufficient data is not available for that topic.
Do not use JSON, lists, or markdown.

User message:
{user_message}
""".strip()


def build_help_prompt(user_message):
    """Prompt for dynamic help/guidance responses about supported query types."""
    return f"""
You are a professional support chatbot for account package data.
The user is asking for help or saying they have queries.
Reply with a short, friendly natural-language message explaining what kinds of questions you can answer.
Include 3 concise example question styles related to package tracking/account data.
Do not use JSON, markdown, or bullet symbols.

User message:
{user_message}
""".strip()


def build_intent_prompt(user_message):
    """Prompt to classify message intent dynamically with LLM."""
    return f"""
Classify the user message into exactly one label.
Allowed labels: greeting, account_id, db, help, out_of_scope

Definitions:
- greeting: salutations like hi/hello/good morning/good night
- account_id: directly asks for account id
- db: asks about package/account shipment data available in track_packages
- help: asks what chatbot can do, asks for assistance, says they have queries/questions without a specific data request
- out_of_scope: unrelated topics outside available data

Output only one label.

User message:
{user_message}
""".strip()


def build_session_data_answer_prompt(user_message, account_id, rows):
    """Prompt for answering DB questions from account-filtered session data only."""
    sample_rows = to_json_safe_rows(rows[:300])
    return f"""
You are a data assistant.
Answer the user's question using ONLY the provided account-filtered dataset.
Do not use external knowledge.
If the dataset does not contain enough information, reply exactly:
I cannot respond because you do not have sufficient data.

Rules:
- Natural language only.
- No JSON, no lists, no markdown.
- Be concise and accurate.
- If the user asks for counts, compute the count from only rows matching the user's condition.
- If the user asks for a specific field/value (carrier, tracking number, status, package id, etc.), provide that exact value from matching rows.
- Respect user filters like carrier names, delivered/status conditions, and date terms such as today.

Account id:
{account_id}

Dataset rows:
{sample_rows}

User question:
{user_message}
""".strip()


def build_request_completed_prompt(user_message):
    """Prompt for dynamic request completion response."""
    return f"""
You are a professional support chatbot.
The user declined to view matching records.
Reply with one short natural-language completion message.
Do not use JSON, lists, or markdown.

User message:
{user_message}
""".strip()


def build_no_data_prompt(user_message, account_id):
    """Prompt for contextual no-data replies when filtered rows are empty."""
    return f"""
You are a support assistant for account package data.
The user asked a data question, but there are zero matching records for account_id {account_id}.
Reply with one short natural-language sentence that references the user's filter context when possible.
Examples of tone:
- No packages were delivered through Amazon for this account.
- No matching package records were found for this account today.
Do not use JSON, lists, or markdown.

User question:
{user_message}
""".strip()


def sql_literal(value):
    """Return a safe SQL literal string for simple generated filters."""
    if value is None:
        return "NULL"
    text_value = str(value)
    if re.fullmatch(r"\d+", text_value):
        return text_value
    return "'" + text_value.replace("'", "''") + "'"

def normalize_generated_sql_for_log(sql_text):
    """Normalize generated SQL for readable, consistent logs."""
    cleaned = str(sql_text or "").strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"^```(?:sql)?", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"```$", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = re.sub(r"(\d)(LIMIT\b)", r"\1 LIMIT", cleaned, flags=re.IGNORECASE)
    return cleaned


def build_account_scoped_candidate_sql(table_name, account_id, schema_map, row_limit=100):
    """Build safe deterministic SQL for a single matched candidate table."""
    resolved_table = str(table_name or "").strip().lower()
    if not resolved_table:
        return "", "candidate_table_missing"

    runtime_tables = set(_get_runtime_supported_tables(schema_map))
    if resolved_table not in runtime_tables:
        return "", "candidate_table_not_supported"

    table_columns_map = _get_supported_columns_from_schema(schema_map, supported_tables=(resolved_table,))
    columns = {str(col).strip().lower() for col in table_columns_map.get(resolved_table, []) if str(col).strip()}
    if "account_id" not in columns:
        return "", "candidate_table_not_account_scoped"

    sql_text = f"SELECT * FROM {resolved_table} WHERE account_id = {sql_literal(account_id)}"

    order_column = ""
    for candidate in ("date_created", "date_added", "updated_at", "created_at", "id"):
        if candidate in columns:
            order_column = candidate
            break
    if order_column:
        sql_text += f" ORDER BY {order_column} DESC"

    if isinstance(row_limit, int) and row_limit > 0:
        sql_text += f" LIMIT {max(1, int(row_limit))}"

    return sql_text, "ok"


def build_backend_fallback_sql(user_question, account_id, schema_map):
    """Build a conservative SQL query on backend when Qwen is unavailable."""
    question = user_question.lower()
    account_id_lit = sql_literal(account_id)
    asks_phone_contact = (
        "phone" in question
        or "cell" in question
        or "mobile" in question
        or "contact number" in question
        or "contact numbers" in question
        or "phone number" in question
        or "phone numbers" in question
    )

    recipient_keywords = (
        "recipient",
        "first name",
        "last name",
        "preferred",
        "email",
        "cell",
        "mobile",
        "phone",
        "username",
        "profile",
        "move in",
        "move out",
        "lease",
    )
    asks_recipient_data = any(keyword in question for keyword in recipient_keywords)
    asks_package_data = any(
        keyword in question
        for keyword in (
            "package",
            "packages",
            "tracking",
            "carrier",
            "delivered",
            "received",
            "shipment",
        )
    )

    # Count-style questions should return deterministic aggregate.
    if _has_count_intent(question):
        if asks_recipient_data and not asks_package_data:
            sql = (
                "SELECT COUNT(*) AS total_records "
                f"FROM core_recipients WHERE account_id = {account_id_lit}"
            )
            return sql, "fallback_count_recipients"

        conditions = [f"account_id = {account_id_lit}"]
        if "delivered" in question:
            conditions.append("date_received IS NOT NULL")
        if "today" in question:
            conditions.append("DATE(date_received) = CURDATE()")
        elif "yesterday" in question:
            conditions.append("DATE(date_received) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        else:
            relative_window_condition = build_relative_time_window_condition(question, "date_received")
            if relative_window_condition:
                conditions.append(relative_window_condition)
        where_sql = " AND ".join(conditions)
        sql = f"SELECT COUNT(*) AS total_records FROM track_packages WHERE {where_sql}"
        return sql, "fallback_count_packages"

    if asks_recipient_data and not asks_package_data:
        recipient_conditions = [f"account_id = {account_id_lit}"]
        if "active" in question:
            recipient_conditions.append("LOWER(COALESCE(recipient_status, '')) = 'active'")
        if "email" in question:
            recipient_conditions.append("COALESCE(email, '') <> ''")
        if asks_phone_contact:
            recipient_conditions.append("COALESCE(cellphone, '') <> ''")

        recipient_where = " AND ".join(recipient_conditions)
        sql = (
            "SELECT recipient_id, account_id, first_name, preferred_first_name, "
            "last_name, email, cellphone, recipient_status "
            "FROM core_recipients "
            f"WHERE {recipient_where} "
            "ORDER BY recipient_id DESC LIMIT 100"
        )
        return sql, "fallback_recipients_select"

    runtime_tables = set(_get_runtime_supported_tables(schema_map))

    # If prompt clearly maps to multiple likely tables, do not auto-pick one.
    # Let caller ask user to choose the exact table name.
    prompt_candidates = detect_prompt_table_candidates(user_question, schema_map, max_tables=6)
    if len(prompt_candidates) > 1:
        return "", "fallback_no_ambiguous_candidates"

    if _is_connect_message_request(user_question):
        preferred_tables = _rank_connect_message_tables(user_question, runtime_tables, max_tables=1)
    else:
        preferred_tables = detect_best_query_tables(user_question, schema_map, max_tables=1)
    target_table = preferred_tables[0] if preferred_tables else ""

    if target_table not in runtime_tables:
        target_table = ""

    if not target_table and asks_package_data and "track_packages" in runtime_tables:
        target_table = "track_packages"

    if not target_table:
        return "", "fallback_no_table_match"

    table_columns_map = _get_supported_columns_from_schema(schema_map, supported_tables=(target_table,))
    columns = {str(col).strip().lower() for col in table_columns_map.get(target_table, []) if str(col).strip()}
    if not columns or "account_id" not in columns:
        return "", f"fallback_no_account_scope:{target_table}"

    conditions = [f"account_id = {account_id_lit}"]

    date_column = ""
    for candidate in ("date_received", "date_created", "date_added", "created_at", "updated_at"):
        if candidate in columns:
            date_column = candidate
            break

    if date_column:
        if "delivered" in question and date_column == "date_received":
            conditions.append("date_received IS NOT NULL")

        if "today" in question:
            conditions.append(f"DATE({date_column}) = CURDATE()")
        elif "yesterday" in question:
            conditions.append(f"DATE({date_column}) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        else:
            relative_window_condition = build_relative_time_window_condition(question, date_column)
            if relative_window_condition:
                conditions.append(relative_window_condition)

    if "shipping_carrier" in columns:
        if "amazon" in question:
            conditions.append("LOWER(COALESCE(shipping_carrier, '')) LIKE '%amazon%'")
        elif "usps" in question or "postal" in question:
            conditions.append(
                "(LOWER(COALESCE(shipping_carrier, '')) LIKE '%usps%' "
                "OR LOWER(COALESCE(shipping_carrier, '')) LIKE '%postal%')"
            )
        elif "fedex" in question:
            conditions.append("LOWER(COALESCE(shipping_carrier, '')) LIKE '%fedex%'")
        elif "ups" in question:
            conditions.append("LOWER(COALESCE(shipping_carrier, '')) LIKE '%ups%'")
        elif "dhl" in question:
            conditions.append("LOWER(COALESCE(shipping_carrier, '')) LIKE '%dhl%'")

    where_sql = " AND ".join(conditions)
    is_count_query = _has_count_intent(question)
    if is_count_query:
        return f"SELECT COUNT(*) AS total_records FROM {target_table} WHERE {where_sql}", f"fallback_count:{target_table}"

    order_field = ""
    for candidate in ("date_received", "date_created", "date_added", "updated_at", "created_at", "id", "package_id", "recipient_id"):
        if candidate in columns:
            order_field = candidate
            break

    order_sql = f" ORDER BY {order_field} DESC" if order_field else ""
    sql = f"SELECT * FROM {target_table} WHERE {where_sql}{order_sql} LIMIT 100"
    return sql, f"fallback_select:{target_table}"


def _tokenize_query_terms(text):
    """Tokenize normalized query text into meaningful terms for table scoring."""
    normalized = normalize_intent_text(text)
    if not normalized:
        return []

    stop_words = {
        "what",
        "which",
        "show",
        "give",
        "tell",
        "about",
        "data",
        "details",
        "record",
        "records",
        "table",
        "tables",
        "for",
        "from",
        "with",
        "this",
        "that",
        "these",
        "those",
        "all",
        "only",
        "the",
        "and",
        "your",
        "my",
        "account",
    }
    tokens = []
    for token in re.findall(r"[a-z0-9_]+", normalized):
        # Keep short but meaningful data terms such as cc/id.
        if len(token) <= 2 and token not in {"cc", "id"}:
            continue
        if token in stop_words:
            continue
        tokens.append(token)
    return tokens


def _table_name_tokens(table_name):
    """Create searchable tokens from a table name."""
    lowered = str(table_name or "").strip().lower()
    if not lowered:
        return set()

    tokens = {lowered, lowered.replace("_", " ")}
    for part in lowered.split("_"):
        if len(part) >= 3:
            tokens.add(part)
            if part.endswith("s") and len(part) >= 4:
                tokens.add(part[:-1])
            elif not part.endswith("s") and len(part) >= 3:
                tokens.add(part + "s")
    return tokens


def _column_alias_tokens(column_name):
    """Create alias tokens from a column name for semantic matching."""
    lowered = str(column_name or "").strip().lower()
    if not lowered:
        return set()

    tokens = {lowered, lowered.replace("_", " ")}
    parts = [part for part in lowered.split("_") if part]
    for part in parts:
        if len(part) >= 3:
            tokens.add(part)
    if len(parts) >= 2 and parts[-1] == "id":
        tokens.add(" ".join(parts[:-1]))
    return tokens


def detect_best_query_tables(user_query, schema_map, max_tables=3):
    """Rank supported account-scoped tables for a natural-language query."""
    runtime_tables = _get_runtime_supported_tables(schema_map)
    if not runtime_tables:
        return []

    query_text = normalize_intent_text(user_query)
    if not query_text:
        return []

    query_terms = _tokenize_query_terms(query_text)
    if not query_terms:
        return []

    billing_intent = any(
        term in query_text
        for term in (
            "cc",
            "cc number",
            "credit card",
            "card number",
            "billing",
            "payment",
            "braintree",
            "paypal",
        )
    )

    table_columns = _get_supported_columns_from_schema(schema_map, supported_tables=runtime_tables)
    scored = []
    for table_name in runtime_tables:
        columns = [str(column).strip().lower() for column in table_columns.get(table_name, [])]
        if "account_id" not in columns:
            continue

        score = 0.0
        table_tokens = _table_name_tokens(table_name)
        column_tokens = set()
        for column in columns:
            column_tokens.update(_column_alias_tokens(column))

        for term in query_terms:
            phrase_term = term.replace("_", " ")
            if term in table_tokens or phrase_term in table_tokens:
                score += 4.0
            if term in column_tokens or phrase_term in column_tokens:
                score += 2.0

        # Hard bias for billing/card-like asks so these do not drift to package tables.
        if billing_intent:
            if table_name == "core_account_billing":
                score += 40.0
            elif table_name == "track_packages":
                score -= 8.0

        if "_" in table_name and table_name.replace("_", " ") in query_text:
            score += 6.0
        if table_name in query_text:
            score += 8.0

        if score > 0:
            scored.append((table_name, score))

    if not scored:
        return []

    scored.sort(key=lambda item: item[1], reverse=True)
    best_score = scored[0][1]
    threshold = max(3.0, best_score * 0.5)
    selected = [table for table, score in scored if score >= threshold][: max(1, int(max_tables or 3))]
    return selected


def detect_explicit_query_tables(user_query, schema_map, max_tables=2):
    """Return explicitly named tables from user query for fast prompt scoping."""
    runtime_tables = _get_runtime_supported_tables(schema_map)
    if not runtime_tables:
        return []

    normalized_text = normalize_intent_text(user_query)
    raw_text = re.sub(r"[^a-z0-9_\s]", " ", str(user_query or "").strip().lower())
    raw_text = re.sub(r"\s+", " ", raw_text).strip()
    search_texts = [text for text in (raw_text, normalized_text) if text]
    if not search_texts:
        return []

    # Natural-language aliases for tables whose canonical names are not typically
    # spoken by users (for example, "mail rooms" -> track_mailrooms).
    alias_patterns_by_table = {
        "track_mailrooms": (
            r"\bmail\s*rooms?\b",
            r"\bmailrooms?\b",
        ),
    }

    alias_matches = []
    runtime_table_set = {str(name or "").strip().lower() for name in runtime_tables}
    for table_name, patterns in alias_patterns_by_table.items():
        if table_name not in runtime_table_set:
            continue
        if any(re.search(pattern, query_text) for pattern in patterns for query_text in search_texts):
            alias_matches.append(table_name)

    if alias_matches:
        matched_aliases = []
        seen_aliases = set()
        for table_name in alias_matches:
            if table_name in seen_aliases:
                continue
            seen_aliases.add(table_name)
            matched_aliases.append(table_name)
        limit = max(1, int(max_tables or 2))
        return matched_aliases[:limit]

    matched_tables = []
    for table_name in runtime_tables:
        table_token = str(table_name or "").strip().lower()
        if not table_token:
            continue

        spaced_token = table_token.replace("_", " ")
        if any(
            re.search(rf"\b{re.escape(table_token)}\b", query_text)
            or re.search(rf"\b{re.escape(spaced_token)}\b", query_text)
            for query_text in search_texts
        ):
            matched_tables.append(table_token)

    # Secondary matching for user phrasing that omits module prefix, such as
    # "service types table" for track_service_types.
    if not matched_tables:
        suffix_matches = []
        for table_name in runtime_tables:
            table_token = str(table_name or "").strip().lower()
            if not table_token or "_" not in table_token:
                continue

            suffix_token = "_".join(table_token.split("_")[1:]).strip("_")
            if not suffix_token:
                continue

            spaced_suffix = suffix_token.replace("_", " ")
            if any(
                re.search(rf"\b{re.escape(suffix_token)}\b", query_text)
                or re.search(rf"\b{re.escape(spaced_suffix)}\b", query_text)
                for query_text in search_texts
            ):
                suffix_matches.append(table_token)

        unique_suffix_matches = []
        seen_suffix = set()
        for table_name in suffix_matches:
            if table_name in seen_suffix:
                continue
            seen_suffix.add(table_name)
            unique_suffix_matches.append(table_name)

        # Keep deterministic behavior: only auto-resolve when suffix match is unique.
        if len(unique_suffix_matches) == 1:
            matched_tables = unique_suffix_matches

    if not matched_tables:
        cue_match = None
        for query_text in search_texts:
            cue_match = re.search(r"\b(?:table|from|in)\s+([a-z0-9_]+)\b", query_text)
            if cue_match:
                break
        if cue_match:
            cue_table = cue_match.group(1).strip().lower()
            if cue_table in runtime_tables:
                matched_tables.append(cue_table)

    def _normalize_phrase_tokens(text):
        tokens = []
        stop_terms = {
            "a",
            "all",
            "data",
            "fetch",
            "for",
            "format",
            "from",
            "get",
            "give",
            "in",
            "me",
            "of",
            "only",
            "please",
            "records",
            "row",
            "rows",
            "show",
            "table",
            "tables",
            "the",
            "view",
        }
        for token in re.findall(r"[a-z0-9_]+", str(text or "")):
            normalized = token.strip().lower()
            if not normalized or normalized in stop_terms:
                continue
            if normalized.endswith("ies") and len(normalized) > 4:
                normalized = normalized[:-3] + "y"
            elif normalized.endswith("s") and len(normalized) > 3:
                normalized = normalized[:-1]
            tokens.append(normalized)
        return tokens

    # Natural-language resolver for prompts like "give me service types table".
    # Chooses the best unique supported table match using token overlap.
    if not matched_tables:
        phrase_candidates = []
        for pattern in (
            r"\b([a-z0-9_\s]{2,80}?)\s+table(?:\s+format)?\b",
            r"\btable\s+([a-z0-9_\s]{2,80}?)\b",
        ):
            for query_text in search_texts:
                for match in re.finditer(pattern, query_text):
                    phrase_text = re.sub(r"\s+", " ", (match.group(1) or "").strip())
                    if phrase_text:
                        phrase_candidates.append(phrase_text)

        scored_candidates = []
        for phrase_text in phrase_candidates:
            phrase_tokens = set(_normalize_phrase_tokens(phrase_text))
            if not phrase_tokens:
                continue

            for table_name in runtime_tables:
                table_token = str(table_name or "").strip().lower()
                if not table_token:
                    continue

                full_name_tokens = set(_normalize_phrase_tokens(table_token.replace("_", " ")))
                suffix_name = "_".join(table_token.split("_")[1:]) if "_" in table_token else table_token
                suffix_tokens = set(_normalize_phrase_tokens(suffix_name.replace("_", " ")))
                candidate_tokens = full_name_tokens | suffix_tokens
                if not candidate_tokens:
                    continue

                overlap = len(phrase_tokens.intersection(candidate_tokens))
                if overlap <= 0:
                    continue

                phrase_text_normalized = " ".join(sorted(phrase_tokens))
                full_text_normalized = " ".join(sorted(full_name_tokens))
                suffix_text_normalized = " ".join(sorted(suffix_tokens))
                ratio = max(
                    SequenceMatcher(None, phrase_text_normalized, full_text_normalized).ratio(),
                    SequenceMatcher(None, phrase_text_normalized, suffix_text_normalized).ratio(),
                )
                score = float(overlap) + ratio
                scored_candidates.append((table_token, score))

        if scored_candidates:
            best_score_by_table = {}
            for table_name, score in scored_candidates:
                if score > best_score_by_table.get(table_name, 0.0):
                    best_score_by_table[table_name] = score

            ranked = sorted(best_score_by_table.items(), key=lambda item: item[1], reverse=True)
            if ranked:
                top_table, top_score = ranked[0]
                second_score = ranked[1][1] if len(ranked) > 1 else 0.0
                # Keep deterministic behavior by requiring clear winner.
                if top_score >= 1.85 and (top_score - second_score) >= 0.35:
                    matched_tables = [top_table]

    if not matched_tables:
        return []

    unique_tables = []
    seen = set()
    for table_name in matched_tables:
        if table_name in seen:
            continue
        seen.add(table_name)
        unique_tables.append(table_name)

    limit = max(1, int(max_tables or 2))
    return unique_tables[:limit]


def _find_exact_table_name_matches(user_query, candidate_tables):
    """Return candidate tables whose canonical/suffixed names are explicitly present in query."""
    normalized_text = normalize_intent_text(user_query)
    raw_text = re.sub(r"[^a-z0-9_\s]", " ", str(user_query or "").strip().lower())
    raw_text = re.sub(r"\s+", " ", raw_text).strip()
    search_texts = [text for text in (raw_text, normalized_text) if text]
    if not search_texts:
        return []

    full_matches = []
    suffix_matches = []
    seen_full = set()
    seen_suffix = set()
    for table_name in candidate_tables or []:
        table_token = str(table_name or "").strip().lower()
        if not table_token:
            continue

        full_variants = {table_token, table_token.replace("_", " ")}
        suffix_variants = set()
        if "_" in table_token:
            suffix_parts = [part for part in table_token.split("_")[1:] if part]
            # Avoid generic one-word suffix matching (for example "settings"),
            # which can make exact matching ambiguous across many tables.
            suffix = "_".join(suffix_parts).strip("_")
            if suffix and len(suffix_parts) >= 2:
                suffix_variants.add(suffix)
                suffix_variants.add(suffix.replace("_", " "))

        matched_full = any(
            re.search(rf"\b{re.escape(variant)}\b", query_text)
            for variant in full_variants
            for query_text in search_texts
        )
        if matched_full:
            if table_token not in seen_full:
                seen_full.add(table_token)
                full_matches.append(table_token)
            continue

        matched_suffix = any(
            re.search(rf"\b{re.escape(variant)}\b", query_text)
            for variant in suffix_variants
            for query_text in search_texts
        )
        if matched_suffix and table_token not in seen_suffix:
            seen_suffix.add(table_token)
            suffix_matches.append(table_token)

    # Prefer strong canonical/full-name matches when available.
    if full_matches:
        return full_matches
    return suffix_matches


def build_table_disambiguation_response(candidate_tables):
    """Build deterministic clarification text listing matched table candidates."""
    cleaned = [str(name).strip() for name in (candidate_tables or []) if str(name).strip()]
    if not cleaned:
        return INTENT_CLARIFICATION_RESPONSE

    preview = ", ".join(cleaned[:6])
    if len(cleaned) > 6:
        preview += ", ..."
    return (
        "I found multiple matching tables for your prompt: "
        f"{preview}. Please specify the exact table name to fetch accurate data."
    )


def _is_ambiguous_marker(value):
    """Return True when a routing source/reason denotes ambiguous table selection."""
    return "ambiguous" in str(value or "").strip().lower()


def detect_prompt_table_candidates(user_query, schema_map, max_tables=6):
    """Return likely table candidates from prompt text for clarification prompts."""
    runtime_tables = _get_runtime_supported_tables(schema_map)
    if not runtime_tables:
        return []

    exact_matches = _find_exact_table_name_matches(user_query, runtime_tables)
    if len(exact_matches) > 1:
        limit = max(2, int(max_tables or 6))
        return exact_matches[:limit]

    query_text = normalize_intent_text(user_query)
    query_terms = _tokenize_query_terms(query_text)
    if not query_terms:
        return []

    generic_terms = {
        "account",
        "accounts",
        "all",
        "data",
        "detail",
        "details",
        "list",
        "record",
        "records",
        "show",
        "table",
        "tables",
        "user",
        "users",
    }

    scored = []
    for table_name in runtime_tables:
        table_tokens = _table_name_tokens(table_name)
        score = 0.0
        matched_terms = set()
        for term in query_terms:
            phrase_term = term.replace("_", " ")
            term_variants = {term, phrase_term}
            if term.endswith("s") and len(term) >= 4:
                term_variants.add(term[:-1])
            elif not term.endswith("s") and len(term) >= 3:
                term_variants.add(term + "s")

            if any(variant in table_tokens for variant in term_variants):
                matched_terms.add(term)
                if term in generic_terms:
                    score += 1.0
                else:
                    score += 3.0

        lowered_table = str(table_name or "").strip().lower()
        if lowered_table and lowered_table.replace("_", " ") in query_text:
            score += 4.0
        if lowered_table and lowered_table in query_text:
            score += 5.0

        # Favor tables that match more distinct prompt terms.
        if matched_terms:
            score += float(len(matched_terms))

        if score > 0:
            scored.append((lowered_table, score, len(matched_terms)))

    if not scored:
        return []

    scored.sort(key=lambda item: (item[1], item[2]), reverse=True)
    best_table, best_score, best_term_hits = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else 0.0

    # If one table clearly wins on score/term coverage, auto-select it.
    if (
        best_term_hits >= 2
        and (best_score - second_score) >= 1.5
    ):
        return [best_table]

    threshold = max(2.5, best_score * 0.75)
    selected = [table for table, score, _ in scored if score >= threshold]
    if len(selected) <= 1:
        return []

    limit = max(2, int(max_tables or 6))
    return selected[:limit]


EXACT_TABLE_PROMPT_SOURCES = {
    "explicit_table_name",
    "dynamic_exact_name",
    "settings_exact_name",
}


def _is_table_name_style_request(user_query, runtime_tables):
    """Return True when prompt is essentially a direct table-name style ask."""
    raw_text = re.sub(r"[^a-z0-9_\s]", " ", str(user_query or "").strip().lower())
    raw_text = re.sub(r"\s+", " ", raw_text).strip()
    if not raw_text:
        return False

    tokens = raw_text.split()
    if len(tokens) > 4:
        return False

    exact_matches = _find_exact_table_name_matches(user_query, runtime_tables)
    if not exact_matches:
        return False

    trigger_words = {"show", "give", "get", "fetch", "list", "table", "data", "details"}
    candidate_phrases = {raw_text}
    for table_name in exact_matches:
        table_token = str(table_name).strip().lower()
        if not table_token:
            continue
        candidate_phrases.add(table_token)
        candidate_phrases.add(table_token.replace("_", " "))
        for trigger in trigger_words:
            candidate_phrases.add(f"{trigger} {table_token}")
            candidate_phrases.add(f"{trigger} {table_token.replace('_', ' ')}")

    return raw_text in candidate_phrases


def select_prompt_tables_for_query(user_query, schema_map, max_tables=3):
    """Select prompt tables with explicit table-name priority for lower latency."""
    runtime_supported_tables = _get_runtime_supported_tables(schema_map)
    if not runtime_supported_tables:
        return tuple(), "none", False

    explicit_tables = detect_explicit_query_tables(
        user_query,
        schema_map,
        max_tables=EXPLICIT_TABLE_MAX_PROMPT_TABLES,
    )
    if explicit_tables:
        return tuple(explicit_tables), "explicit_table_name", True

    exact_runtime_matches = _find_exact_table_name_matches(user_query, runtime_supported_tables)
    if len(exact_runtime_matches) == 1:
        return (exact_runtime_matches[0],), "dynamic_exact_name", True
    if len(exact_runtime_matches) > 1:
        limit = max(1, int(max_tables or 3))
        return tuple(exact_runtime_matches[:limit]), "dynamic_ambiguous_exact", False

    if _is_settings_request(user_query):
        ranked_settings_tables = _rank_settings_tables(user_query, runtime_supported_tables, max_tables=max_tables)
        if ranked_settings_tables:
            exact_settings_matches = _find_exact_table_name_matches(user_query, ranked_settings_tables)
            limit = max(1, int(max_tables or 3))
            if len(exact_settings_matches) == 1:
                return (exact_settings_matches[0],), "settings_exact_name", False
            if len(exact_settings_matches) > 1:
                return tuple(exact_settings_matches[:limit]), "settings_ambiguous_exact", False
            if len(ranked_settings_tables) > 1:
                return tuple(ranked_settings_tables[:limit]), "settings_ambiguous", False
            return tuple(ranked_settings_tables[:limit]), "settings_intent", False

    query_text = normalize_intent_text(user_query)
    billing_intent = any(
        term in query_text
        for term in (
            "billing",
            "account billing",
            "cc",
            "cc number",
            "credit card",
            "card number",
            "payment",
        )
    )
    if billing_intent and "core_account_billing" in set(runtime_supported_tables):
        return ("core_account_billing",), "billing_intent_fast_path", True

    if _is_connect_message_request(user_query):
        selected_connect_tables = _rank_connect_message_tables(
            user_query,
            runtime_supported_tables,
            max_tables=max_tables,
        )
        if selected_connect_tables:
            limit = max(1, int(max_tables or 3))
            return tuple(selected_connect_tables[:limit]), "connect_message_intent", False

    if should_use_recipient_accuracy_path(user_query):
        runtime_table_set = set(runtime_supported_tables)
        if "track_packages" in runtime_table_set and "core_recipients" in runtime_table_set:
            return ("track_packages", "core_recipients"), "recipient_pair", False

    preferred_tables = detect_best_query_tables(user_query, schema_map, max_tables=max_tables)
    if preferred_tables:
        if len(preferred_tables) > 1 and query_requests_table_view(user_query):
            limit = max(1, int(max_tables or 3))
            return tuple(preferred_tables[:limit]), "dynamic_ambiguous", False
        return tuple(preferred_tables), "semantic_ranked", False

    return runtime_supported_tables, "runtime_default", False


def _is_simple_table_direct_request(user_query):
    """Return True when query can use deterministic single-table SQL fast path."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    if _find_exact_table_name_matches(user_query, SUPPORTED_QUERY_TABLES):
        return True

    explicit_data_intent = detect_explicit_data_intent(text)
    if explicit_data_intent != "none":
        return True

    def has_count_intent(message_text):
        return _has_count_intent(message_text)

    # Allow direct fast path for carrier aggregate chart prompts.
    has_carrier_subject = _has_carrier_subject(text)
    has_package_subject = any(token in text for token in TOP_RECIPIENT_PACKAGE_TERMS)
    has_chart_word = any(token in text for token in ("chart", "bar", "graph", "plot"))
    if has_carrier_subject and has_package_subject and (has_chart_word or has_count_intent(text) or "maximum" in text):
        return True

    blocked_terms = (
        "join",
        "compare",
        "comparison",
        "trend",
        "distribution",
        "percentage",
        "percent",
        "chart",
        "graph",
        "plot",
    )
    if any(term in text for term in blocked_terms):
        return False

    if (
        query_requests_table_view(text)
        and any(term in text for term in ("latest", "last", "record", "records", "package", "packages", "data"))
    ):
        return True

    if has_count_intent(text):
        return True

    return any(
        term in text
        for term in (
            "details",
            "detail",
            "list",
            "records",
            "record",
            "show",
            "data",
            "table",
            "all",
            "latest",
        )
    )


def _query_mentions_table_token(user_query, table_name):
    """Return True when query includes table name token or spaced form."""
    text = normalize_intent_text(user_query)
    table_token = str(table_name or "").strip().lower()
    if not text or not table_token:
        return False

    if table_token in text or table_token.replace("_", " ") in text:
        return True

    parts = [part for part in table_token.split("_") if len(part) >= 4]
    return any(re.search(rf"\b{re.escape(part)}\b", text) for part in parts)


def detect_direct_target_table(user_query, schema_map, prompt_tables=None, prompt_table_source=""):
    """Resolve target table for deterministic direct SQL when table/keyword is explicit."""
    runtime_tables = set(_get_runtime_supported_tables(schema_map))
    if not runtime_tables:
        return "", "none"

    explicit_tables = detect_explicit_query_tables(user_query, schema_map, max_tables=1)
    if explicit_tables:
        return explicit_tables[0], "explicit_table_name"

    exact_runtime_matches = _find_exact_table_name_matches(user_query, sorted(runtime_tables))
    if len(exact_runtime_matches) == 1:
        return exact_runtime_matches[0], "dynamic_exact_name"
    if len(exact_runtime_matches) > 1:
        return "", "dynamic_exact_ambiguous"

    if _is_settings_request(user_query):
        runtime_settings_tables = [name for name in sorted(runtime_tables) if str(name).endswith("_settings")]
        exact_settings_matches = _find_exact_table_name_matches(user_query, runtime_settings_tables)
        if len(exact_settings_matches) == 1:
            return exact_settings_matches[0], "settings_exact_name"
        if len(exact_settings_matches) > 1:
            return "", "settings_exact_ambiguous"

        ranked_settings_tables = _rank_settings_tables(user_query, runtime_tables, max_tables=3)
        if len(ranked_settings_tables) == 1:
            return ranked_settings_tables[0], "settings_keyword_hint"
        if len(ranked_settings_tables) > 1:
            return "", "settings_intent_ambiguous"
        return "", "settings_intent_no_table_match"

    text = normalize_intent_text(user_query)
    if _is_connect_message_request(user_query):
        connect_candidates = _rank_connect_message_tables(user_query, runtime_tables, max_tables=6)
        for table_name in connect_candidates:
            if table_name in runtime_tables:
                return table_name, "connect_message_keyword_hint"

        # Prevent unrelated fallback when connect message intent is explicit.
        return "", "connect_intent_no_table_match"
    keyword_hints = (
        (
            "core_account_billing",
            ("billing", "credit card", "card", "cc", "payment", "braintree", "paypal", "invoice"),
        ),
        (
            "core_recipients",
            ("recipient", "recipients", "email", "cellphone", "phone", "mobile", "status", "active", "inactive"),
        ),
        (
            "track_packages",
            ("package", "packages", "tracking", "carrier", "delivery", "delivered", "received", "shipment"),
        ),
        (
            "guest_ip_date",
            ("guest", "ip", "login ip", "ip date"),
        ),
    )
    for table_name, terms in keyword_hints:
        if table_name in runtime_tables and any(term in text for term in terms):
            return table_name, "keyword_hint"

    # Dynamic table resolution for all supported tables on simple direct asks.
    semantic_candidates = detect_best_query_tables(user_query, schema_map, max_tables=3)
    if len(semantic_candidates) == 1 and semantic_candidates[0] in runtime_tables:
        return semantic_candidates[0], "semantic_hint"
    if len(semantic_candidates) > 1 and query_requests_table_view(user_query):
        return "", "dynamic_intent_ambiguous"

    if isinstance(prompt_tables, tuple) and len(prompt_tables) == 1:
        candidate = prompt_tables[0]
        if candidate in runtime_tables and (
            prompt_table_source in ("explicit_table_name", "billing_intent_fast_path")
            or _query_mentions_table_token(user_query, candidate)
            or (
                prompt_table_source in ("semantic_ranked", "connect_message_intent")
                and query_requests_table_view(user_query)
            )
        ):
            return candidate, f"prompt_source:{prompt_table_source or 'single'}"

    # Dynamic default to track_packages only for package-centric intents.
    package_context_tokens = (
        "package",
        "packages",
        "tracking",
        "carrier",
        "delivery",
        "delivered",
        "shipment",
    )
    if "track_packages" in runtime_tables and any(token in text for token in package_context_tokens):
        return "track_packages", "default_track_packages"

    return "", "none"


def build_direct_table_fast_sql(account_id, user_query, target_table, schema_map, row_limit=100):
    """Build deterministic direct SQL for single-table requests without LLM."""
    table_name = str(target_table or "").strip().lower()
    if not table_name:
        return ""

    table_columns_map = _get_supported_columns_from_schema(schema_map, supported_tables=(table_name,))
    columns = [str(col).strip().lower() for col in table_columns_map.get(table_name, [])]
    if not columns:
        return ""

    account_id_lit = sql_literal(account_id)
    text = normalize_intent_text(user_query)

    # Carrier aggregate prompts should not fall back to generic table counts.
    if table_name == "track_packages":
        if is_carrier_percentage_request(text):
            return build_carrier_percentage_sql(account_id, user_query, row_limit=row_limit)
        if is_top_carrier_request(text) or is_carrier_wise_count_request(text):
            return build_carrier_count_sql(account_id, user_query, row_limit=row_limit)
    if table_name == "track_shipping_carriers":
        if is_carrier_percentage_request(text):
            return build_carrier_percentage_sql(account_id, user_query, row_limit=row_limit)
        if is_top_carrier_request(text) or is_carrier_wise_count_request(text):
            return build_carrier_count_sql(account_id, user_query, row_limit=row_limit)

    conditions = []
    if "account_id" in columns:
        conditions.append(f"account_id = {account_id_lit}")

    if "delivered" in text and "date_received" in columns:
        conditions.append("date_received IS NOT NULL")

    if "today" in text and "date_received" in columns:
        conditions.append("DATE(date_received) = CURDATE()")
    elif "yesterday" in text and "date_received" in columns:
        conditions.append("DATE(date_received) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
    elif "date_received" in columns:
        relative_window_condition = build_relative_time_window_condition(text, "date_received")
        if relative_window_condition:
            conditions.append(relative_window_condition)

    where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    is_count_query = (
        re.search(r"\bcount\b", text) is not None
        or re.search(r"\btotal\b", text) is not None
        or "how many" in text
        or "number of" in text
    )
    if is_count_query:
        return f"SELECT COUNT(*) AS total_records FROM {table_name}{where_sql}"

    requested_limit = extract_requested_row_limit(user_query, max_limit=200)
    safe_limit = requested_limit if requested_limit is not None else (100 if row_limit is None else max(1, min(int(row_limit), 200)))

    details_intent = any(
        term in text
        for term in (
            "details",
            "detail",
            "list",
            "records",
            "record",
            "table",
            "all",
            "data",
        )
    )
    open_data_intent = is_open_table_data_request(user_query)
    if details_intent or open_data_intent:
        select_sql = "*"
    else:
        requested_fields = infer_requested_fields_from_query(user_query)
        selected_fields = [field for field in requested_fields if str(field).strip().lower() in set(columns)]
        if selected_fields:
            select_sql = ", ".join(dict.fromkeys(selected_fields))
        else:
            select_sql = "*"

    order_field = ""
    for candidate in ("date_received", "date_added", "date_created", "updated_at", "created_at", "id", "package_id", "recipient_id"):
        if candidate in columns:
            order_field = candidate
            break

    order_sql = f" ORDER BY {order_field} DESC" if order_field else ""
    return f"SELECT {select_sql} FROM {table_name}{where_sql}{order_sql} LIMIT {safe_limit}"


def build_supported_schema_text(schema_map, supported_tables=None):
    """Return prompt schema text constrained to supported query tables."""
    selected_schema = {}
    runtime_tables = tuple(supported_tables or _get_runtime_supported_tables(schema_map))
    table_columns = _get_supported_columns_from_schema(schema_map, supported_tables=runtime_tables)

    for table_name in runtime_tables:
        selected_schema[table_name] = table_columns.get(table_name, [])

    if any(columns for columns in selected_schema.values()):
        return format_schema_for_prompt(selected_schema)

    fallback_lines = []
    for table_name in runtime_tables:
        if table_name == "track_packages":
            fallback_lines.append("track_packages(account_id, ...)")
        elif table_name == "core_recipients":
            fallback_lines.append("core_recipients(account_id, recipient_id, ...)")
        else:
            fallback_lines.append(f"{table_name}(...)")
    return "\n".join(fallback_lines)


def get_allowed_columns_text(schema_map, supported_tables=None):
    """Return qualified allowed columns text from live schema when available."""
    runtime_tables = tuple(supported_tables or _get_runtime_supported_tables(schema_map))
    table_columns = _get_supported_columns_from_schema(schema_map, supported_tables=runtime_tables)
    qualified_columns = []
    for table_name in runtime_tables:
        for column in table_columns.get(table_name, []):
            qualified_columns.append(f"{table_name}.{column}")
    return ", ".join(qualified_columns)


def is_supported_tables_account_scoped(sql_query, account_id):
    """Validate query scope uses only supported tables and current account_id."""
    normalized = str(sql_query or "").strip().lower()
    if not normalized:
        return False, "empty_sql"

    table_pattern = re.compile(r"\b(?:from|join)\s+([a-zA-Z0-9_]+)")
    referenced_tables = {
        match.group(1).strip().lower()
        for match in table_pattern.finditer(normalized)
        if match.group(1).strip()
    }
    schema_map = fetch_schema_metadata_for_chatbot(force_refresh=False)
    runtime_tables = _get_runtime_supported_tables(schema_map)
    supported_tables = set(runtime_tables)
    unsupported = referenced_tables - supported_tables
    if unsupported:
        return False, f"unsupported_tables:{','.join(sorted(unsupported))}"

    if not referenced_tables:
        return False, "missing_supported_table_reference"

    if "account_id" not in normalized:
        return False, "missing_account_id_filter"

    account_id_str = str(account_id).strip().lower()
    if account_id_str and account_id_str not in normalized:
        return False, "missing_specific_account_id_value"

    account_value_pattern = rf"'?{re.escape(account_id_str)}'?"

    supported_columns = _get_supported_columns_from_schema(schema_map, supported_tables=runtime_tables)

    table_alias_pattern = re.compile(
        r"\b(?:from|join)\s+([a-zA-Z0-9_]+)(?:\s+(?:as\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?"
    )
    alias_map = {}
    reserved_alias_tokens = {
        "on",
        "where",
        "group",
        "order",
        "limit",
        "left",
        "right",
        "inner",
        "outer",
        "cross",
        "having",
    }
    for match in table_alias_pattern.finditer(normalized):
        table_name = (match.group(1) or "").strip().lower()
        alias_name = (match.group(2) or "").strip().lower()
        if not table_name:
            continue
        if alias_name and alias_name not in reserved_alias_tokens:
            alias_map.setdefault(table_name, set()).add(alias_name)

    for table_name in referenced_tables:
        table_columns = [str(column).strip().lower() for column in supported_columns.get(table_name, [])]
        if "account_id" not in table_columns:
            continue

        if not account_id_str:
            return False, f"missing_{table_name}_account_scope"

        qualifiers = {table_name}
        qualifiers.update(alias_map.get(table_name, set()))
        qualifier_patterns = [
            rf"\b{re.escape(qualifier)}\s*\.\s*account_id\s*=\s*{account_value_pattern}"
            for qualifier in qualifiers
        ]
        qualifier_patterns.append(rf"\baccount_id\s*=\s*{account_value_pattern}")
        if not any(re.search(pattern, normalized) for pattern in qualifier_patterns):
            return False, f"missing_{table_name}_account_scope"

    return True, "ok"


def _extract_referenced_tables(sql_query):
    """Extract table names referenced in FROM/JOIN clauses."""
    normalized = str(sql_query or "").strip().lower()
    if not normalized:
        return []

    table_pattern = re.compile(r"\b(?:from|join)\s+([a-zA-Z0-9_]+)")
    tables = []
    seen = set()
    for match in table_pattern.finditer(normalized):
        table_name = (match.group(1) or "").strip().lower()
        if not table_name or table_name in seen:
            continue
        seen.add(table_name)
        tables.append(table_name)
    return tables


def _rewrite_mysql_sql_for_sqlite(sql_text):
    """Best-effort rewrite of generated MySQL SQL into SQLite-compatible SQL."""
    rewritten = str(sql_text or "").strip()
    if not rewritten:
        return ""

    rewritten = rewritten.replace("`", "")

    # DATE_SUB(CURDATE(), INTERVAL N UNIT) -> DATE('now', '-N unit') for SQLite.
    def _date_sub_repl(match):
        value = int(match.group(1))
        unit = str(match.group(2) or "DAY").upper()
        if unit == "WEEK":
            return f"DATE('now', '-{value * 7} day')"
        if unit == "MONTH":
            return f"DATE('now', '-{value} month')"
        if unit == "YEAR":
            return f"DATE('now', '-{value} year')"
        return f"DATE('now', '-{value} day')"

    rewritten = re.sub(
        r"DATE_SUB\s*\(\s*CURDATE\s*\(\s*\)\s*,\s*INTERVAL\s+(\d+)\s+(DAY|WEEK|MONTH|YEAR)\s*\)",
        _date_sub_repl,
        rewritten,
        flags=re.IGNORECASE,
    )

    rewritten = re.sub(r"\bCURDATE\s*\(\s*\)", "DATE('now')", rewritten, flags=re.IGNORECASE)
    rewritten = re.sub(
        r"YEAR\s*\(\s*([a-zA-Z_][a-zA-Z0-9_\.]*?)\s*\)",
        r"CAST(strftime('%Y', \1) AS INTEGER)",
        rewritten,
        flags=re.IGNORECASE,
    )
    rewritten = re.sub(
        r"DATE\s*\(\s*([a-zA-Z_][a-zA-Z0-9_\.]*?)\s*\)",
        r"DATE(\1)",
        rewritten,
        flags=re.IGNORECASE,
    )
    return rewritten


def _fetch_account_dataset_rows(table_name, account_id, row_limit=ACCOUNT_DATASET_MAX_ROWS):
    """Fetch account-scoped rows for one supported table as dataset source."""
    safe_table = str(table_name or "").strip().lower()
    if not safe_table or not re.fullmatch(r"[a-z_][a-z0-9_]*", safe_table):
        return [], "invalid_table_name"

    safe_limit = max(1, min(int(row_limit or ACCOUNT_DATASET_MAX_ROWS), ACCOUNT_DATASET_MAX_ROWS))
    sql_text = (
        f"SELECT * FROM {safe_table} "
        f"WHERE account_id = {sql_literal(account_id)} "
        f"LIMIT {safe_limit}"
    )
    return execute_read_only_sql_for_chatbot(sql_text, max_rows=safe_limit)


def execute_sql_with_account_datasets(sql_text, account_id, schema_map, max_rows=100):
    """Execute generated SQL against account-scoped in-memory datasets when possible."""
    if not USE_ACCOUNT_DATASET_EXECUTION:
        return [], "dataset_disabled", False

    referenced_tables = _extract_referenced_tables(sql_text)
    if not referenced_tables:
        return [], "dataset_no_referenced_tables", False

    runtime_tables = set(_get_runtime_supported_tables(schema_map))
    if any(table_name not in runtime_tables for table_name in referenced_tables):
        return [], "dataset_unsupported_tables", False

    supported_columns = _get_supported_columns_from_schema(schema_map, supported_tables=tuple(referenced_tables))
    for table_name in referenced_tables:
        table_columns = [str(column).strip().lower() for column in supported_columns.get(table_name, [])]
        if "account_id" not in table_columns:
            return [], f"dataset_missing_account_scope:{table_name}", False

    sqlite_sql = _rewrite_mysql_sql_for_sqlite(sql_text)
    if not sqlite_sql:
        return [], "dataset_empty_sql", False

    try:
        connection = sqlite3.connect(":memory:")
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()

        for table_name in referenced_tables:
            dataset_rows, dataset_status = _fetch_account_dataset_rows(
                table_name,
                account_id,
                row_limit=ACCOUNT_DATASET_MAX_ROWS,
            )
            if dataset_status != "ok":
                connection.close()
                return [], f"dataset_fetch_failed:{table_name}:{dataset_status}", False

            column_names = []
            for row in dataset_rows:
                if not isinstance(row, dict):
                    continue
                for key in row.keys():
                    normalized = str(key).strip()
                    if normalized and normalized not in column_names:
                        column_names.append(normalized)

            if not column_names:
                column_names = [str(column).strip() for column in supported_columns.get(table_name, []) if str(column).strip()]
            if "account_id" not in [column.lower() for column in column_names]:
                column_names.insert(0, "account_id")

            column_defs = ", ".join(f'"{column}" TEXT' for column in column_names)
            cursor.execute(f'CREATE TABLE "{table_name}" ({column_defs})')

            if dataset_rows:
                placeholders = ", ".join("?" for _ in column_names)
                quoted_columns = ", ".join(f'"{column}"' for column in column_names)
                insert_sql = f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})'

                insert_values = []
                for row in dataset_rows:
                    if not isinstance(row, dict):
                        continue
                    row_values = []
                    for column in column_names:
                        value = row.get(column)
                        if value is None or isinstance(value, (str, int, float, bool)):
                            row_values.append(value)
                        else:
                            row_values.append(str(value))
                    insert_values.append(tuple(row_values))
                if insert_values:
                    cursor.executemany(insert_sql, insert_values)

        cursor.execute(sqlite_sql)
        result_rows = [dict(row) for row in cursor.fetchall()]
        connection.close()

        if isinstance(max_rows, int) and max_rows > 0:
            result_rows = result_rows[:max_rows]
        return result_rows, "ok", True
    except Exception as error:
        logger.warning("DATASET_EXECUTION | failed | error=%s", error)
        return [], f"dataset_execution_failed:{error}", False


def execute_query_with_dataset_fallback(sql_text, account_id, schema_map, max_rows=100):
    """Execute query from account datasets first, then fallback to DB execution."""
    dataset_rows, dataset_status, dataset_used = execute_sql_with_account_datasets(
        sql_text,
        account_id,
        schema_map,
        max_rows=max_rows,
    )
    if dataset_used and dataset_status == "ok":
        return dataset_rows, "ok", "dataset"

    db_rows, db_status = execute_read_only_sql_for_chatbot(sql_text, max_rows=max_rows)
    source = "db"
    if dataset_status.startswith("dataset_") and dataset_status != "dataset_disabled":
        source = f"db_after_{dataset_status}"
    return db_rows, db_status, source


def build_backend_fallback_answer(rows):
    """Create natural-language answer from result rows without LLM."""
    if not rows:
        return "I could not find matching organizational data for your request."

    sample = _mask_row_for_display(rows[0]) if rows else {}
    if "total_records" in sample:
        return f"The total matching records are {sample.get('total_records', 0)}."

    preview_parts = []
    for key, value in list(sample.items())[:4]:
        preview_parts.append(f"{key}={value}")
    preview = ", ".join(preview_parts)
    return (
        f"I found {len(rows)} matching record(s). "
        f"Here is a quick preview from the latest record: {preview}."
    )


def clean_chatbot_answer(answer, rows):
    """Normalize assistant output into plain natural language."""
    text = str(answer or "").strip()
    if not text:
        return build_backend_fallback_answer(rows)

    # Remove forced prefixes from old prompts.
    lowered = text.lower()
    if lowered.startswith("notifii chatbot:"):
        text = text.split(":", 1)[1].strip()

    # If model returned structured payload, replace with natural-language fallback.
    structured_like = (
        re.fullmatch(r"\s*[\[{].*[\]}]\s*", text, flags=re.DOTALL) is not None
        or ("{" in text and "}" in text and ":" in text and len(text.split()) <= 25)
    )
    if structured_like:
        return build_backend_fallback_answer(rows)

    return text


def _fuzzy_best_token_match(token, vocabulary, threshold=0.78):
    """Return closest vocabulary token if similarity is above threshold."""
    if len(token) <= 2:
        return token

    if token in NORMALIZATION_VOCAB_SET:
        return token

    best_word = token
    best_score = 0.0
    for candidate in vocabulary:
        score = SequenceMatcher(None, token, candidate).ratio()
        if score > best_score:
            best_score = score
            best_word = candidate
    if best_score >= threshold:
        return best_word
    return token


def _expand_short_token_match(token, vocabulary):
    """Resolve 3-letter typo/truncation tokens with conservative dynamic matching."""
    normalized = str(token or "").strip().lower()
    if len(normalized) != 3 or normalized in SHORT_TOKEN_FUZZY_BLOCKLIST:
        return normalized

    candidates = []
    for candidate in vocabulary:
        cand = str(candidate or "").strip().lower()
        if len(cand) < 4:
            continue
        if cand.startswith(normalized) and (len(cand) - len(normalized)) <= 2:
            candidates.append(cand)

    # Only expand when there is a single clear prefix candidate.
    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        return normalized

    scored = sorted(
        ((cand, SequenceMatcher(None, normalized, cand[: len(normalized)]).ratio()) for cand in candidates),
        key=lambda item: item[1],
        reverse=True,
    )
    top_word, top_score = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else 0.0
    if top_score >= 0.95 and (top_score - second_score) >= 0.08:
        return top_word
    return normalized


def _has_db_intent_signal(text):
    """Return True when text strongly indicates a DB/data request, including typo-heavy prompts."""
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False

    if any(term in normalized for term in DB_INTENT_TERMS):
        return True

    # Lightweight fuzzy fallback for typo variants like "dat" -> "data".
    tokens = [token for token in re.findall(r"[a-z0-9_]+", normalized) if len(token) >= 3]
    for token in tokens:
        expanded = _expand_short_token_match(token, DB_INTENT_TERMS)
        if expanded != token and expanded in DB_INTENT_TERMS:
            return True

        for term in DB_INTENT_TERMS:
            term_text = str(term or "").strip().lower()
            if not term_text or " " in term_text or len(term_text) < 4:
                continue
            if SequenceMatcher(None, token, term_text).ratio() >= 0.88:
                return True

    return False


@lru_cache(maxsize=8192)
def _fuzzy_cached_token_match(token, vocab_version):
    """Cache fuzzy token match results per active vocabulary version."""
    _ = vocab_version
    return _fuzzy_best_token_match(token, NORMALIZATION_VOCAB)


@lru_cache(maxsize=4096)
def _normalize_intent_text_cached(raw_text, vocab_version):
    """Cache normalized query text per active vocabulary version."""
    _ = vocab_version
    raw_text = re.sub(r"([a-z])\1{2,}", r"\1\1", raw_text)
    raw_text = re.sub(r"[^a-z0-9\s]", " ", raw_text)
    tokens = [part for part in raw_text.split() if part]

    normalized_tokens = []
    for token in tokens:
        mapped = TYPO_MAP.get(token, token)
        # Avoid aggressive fuzzy remapping for very short words (for example
        # "can" -> "scan"), which can incorrectly trigger OCR intent.
        if mapped in NORMALIZATION_VOCAB_SET or len(mapped) <= 2:
            normalized_tokens.append(mapped)
        elif len(mapped) == 3:
            normalized_tokens.append(_expand_short_token_match(mapped, NORMALIZATION_VOCAB))
        else:
            normalized_tokens.append(_fuzzy_cached_token_match(mapped, vocab_version))

    return " ".join(normalized_tokens)


def normalize_intent_text(user_query):
    """Normalize text and correct common spelling mistakes for intent detection."""
    raw_text = str(user_query or "").strip().lower()
    if not raw_text:
        return ""
    return _normalize_intent_text_cached(raw_text, NORMALIZATION_VOCAB_VERSION)


def classify_user_intent(user_query):
    """Classify intent with local fast rules only (no LLM)."""
    text = normalize_intent_text(user_query)

    if _find_exact_table_name_matches(user_query, SUPPORTED_QUERY_TABLES):
        return "db"

    def is_greeting_only(message_text):
        compact = re.sub(r"\s+", " ", str(message_text or "").strip().lower())
        return any(re.fullmatch(pattern, compact) for pattern in GREETING_PATTERNS)

    def is_capability_help_question(message_text):
        tokens = [part for part in str(message_text or "").split() if part]
        if not tokens:
            return False

        has_subject = any(token in HELP_SUBJECT_TERMS for token in tokens)
        has_action = any(token in HELP_ACTION_TERMS for token in tokens)
        has_context = any(token in HELP_CONTEXT_TERMS for token in tokens)

        # Broad dynamic rule for questions like:
        # "what can you do", "what are your capabilities", "how does this bot work".
        if has_subject and (has_action or has_context):
            return True
        if has_context and any(token in ("what", "how") for token in tokens):
            return True
        return False

    if re.search(r"\baccount\s*id\b", text):
        return "account_id"
    if is_greeting_only(text):
        return "greeting"
    if _has_db_intent_signal(text):
        return "db"
    if is_capability_help_question(text):
        return "help"
    if any(x in text for x in HELP_TERMS):
        return "help"

    return "out_of_scope"


def detect_explicit_data_intent(user_query):
    """Detect direct field/value questions that should get precise answers."""
    text = normalize_intent_text(user_query)
    if not text:
        return "none"

    if is_recipient_wise_count_request(text):
        return "recipient_wise_count"

    if "tracking number" in text or "tracking no" in text:
        return "tracking_number"

    if "package id" in text or "packageid" in text:
        return "package_id"

    if "carrier" in text or "shipping carrier" in text:
        return "shipping_carrier"

    if "status" in text:
        return "status"

    if _has_count_intent(text):
        return "count"

    return "none"


def is_recipient_wise_count_request(user_query):
    """Return True for prompts asking count grouped by recipient."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    has_recipient = _contains_any_term(text, RECIPIENT_WISE_RECIPIENT_TERMS)
    has_count = _contains_any_term(text, RECIPIENT_WISE_COUNT_TERMS)
    has_grouping = _contains_any_term(text, RECIPIENT_WISE_GROUP_TERMS)
    has_package_context = _contains_any_term(text, TOP_RECIPIENT_PACKAGE_TERMS)

    # Recipient-wise package count intent must mention package context;
    # otherwise status/count prompts can be misrouted here.
    return has_recipient and has_count and has_grouping and has_package_context


def is_top_recipient_request(user_query):
    """Return True for prompts asking recipient ranking by package volume."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    # Package-id specific asks should not be routed to recipient ranking intent.
    if query_mentions_package_id(text):
        return False

    subject_terms = set(RECIPIENT_WISE_RECIPIENT_TERMS) | set(TOP_RECIPIENT_SUBJECT_TERMS)
    has_recipient = any(token in text for token in subject_terms)
    has_top = any(token in text for token in TOP_RECIPIENT_TERMS)
    has_low = any(token in text for token in TOP_RECIPIENT_LOW_TERMS)
    has_package_context = any(token in text for token in TOP_RECIPIENT_PACKAGE_TERMS)
    return has_recipient and has_package_context and (has_top or has_low)


def is_low_package_count_request(user_query):
    """Return True when query asks for least/lowest recipient package counts."""
    text = normalize_intent_text(user_query)
    if not text:
        return False
    return any(token in text for token in TOP_RECIPIENT_LOW_TERMS)


def is_top_carrier_request(user_query):
    """Return True for prompts asking carrier ranking by package volume."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    has_carrier = _has_carrier_subject(text)
    has_top = any(token in text for token in TOP_RECIPIENT_TERMS)
    has_low = any(token in text for token in TOP_RECIPIENT_LOW_TERMS)
    has_package_context = any(token in text for token in TOP_RECIPIENT_PACKAGE_TERMS)
    return has_carrier and has_package_context and (has_top or has_low)


def is_carrier_wise_count_request(user_query):
    """Return True for prompts asking package counts grouped by each carrier."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    # Percentage/share prompts should route to the dedicated percentage handler.
    if is_carrier_percentage_request(text):
        return False

    has_carrier = _has_carrier_subject(text)
    has_count = _contains_any_term(text, RECIPIENT_WISE_COUNT_TERMS)
    has_grouping = (
        _contains_any_term(text, RECIPIENT_WISE_GROUP_TERMS)
        or " by " in f" {text} "
        or "group" in text
    )
    has_package_context = any(token in text for token in TOP_RECIPIENT_PACKAGE_TERMS)
    return has_carrier and has_count and has_grouping and has_package_context


def is_carrier_percentage_request(user_query):
    """Return True for prompts asking carrier-wise percentage/share distribution."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    has_carrier = _has_carrier_subject(text)
    has_percentage = any(
        token in text
        for token in (
            "percentage",
            "percentages",
            "percent",
            "perecentage",
            "precentage",
            "share",
            "distribution",
            "breakdown",
            "ratio",
        )
    )
    has_grouping = (
        _contains_any_term(text, RECIPIENT_WISE_GROUP_TERMS)
        or " each " in f" {text} "
        or " by " in f" {text} "
    )
    has_package_context = any(token in text for token in TOP_RECIPIENT_PACKAGE_TERMS)
    plural_carrier_context = "carriers" in text or "shipping carriers" in text
    return has_carrier and has_percentage and (has_grouping or has_package_context or plural_carrier_context)


def is_account_billing_request(user_query):
    """Return True for account billing/payment/cc related prompts."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    billing_terms = (
        "cc number",
        "credit card",
        "card number",
        "billing",
        "billing method",
        "billing email",
        "payment terms",
        "paypal payment",
        "braintree",
    )
    return any(term in text for term in billing_terms)


def _is_connect_message_request(user_query):
    """Return True when query explicitly asks for connect message data."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    has_connect = "connect" in text
    has_message_context = _contains_any_term(text, CONNECT_MESSAGE_CONTEXT_TERMS)
    return has_connect and has_message_context


def _is_settings_request(user_query):
    """Return True when query is asking for settings-related data."""
    text = normalize_intent_text(user_query)
    if not text:
        return False
    return _contains_any_term(text, ("setting", "settings"))


def _rank_settings_tables(user_query, runtime_tables, max_tables=3):
    """Rank settings tables using module hints from user query."""
    text = normalize_intent_text(user_query)
    if not text:
        return []

    runtime_set = {str(name or "").strip().lower() for name in (runtime_tables or [])}
    if not runtime_set:
        return []

    settings_tables = [table_name for table_name in sorted(runtime_set) if table_name.endswith("_settings")]
    if not settings_tables:
        return []

    exact_matches = _find_exact_table_name_matches(user_query, settings_tables)
    if exact_matches:
        limit = max(1, int(max_tables or 3))
        return exact_matches[:limit]

    module_hints = (
        ("connect_settings", ("connect", "message", "messages", "sms", "email", "text")),
        ("checkout_settings", ("checkout", "check out", "reservation", "facility")),
        ("track_settings", ("track", "tracking", "package", "packages", "carrier")),
        ("locker_settings", ("locker", "lockers", "pin")),
        ("corporate_settings", ("corporate", "company", "organization")),
        ("core_user_settings", ("user", "users", "profile", "login")),
        ("core_account_settings", ("account", "accounts", "account level")),
    )

    scored = []
    for table_name in settings_tables:
        score = 8.0
        spaced_name = table_name.replace("_", " ")

        if table_name in text:
            score += 40.0
        if spaced_name in text:
            score += 35.0

        for hinted_table, hint_terms in module_hints:
            if table_name != hinted_table:
                continue
            if _contains_any_term(text, hint_terms):
                score += 24.0

        # Prefer connect settings for generic "settings" asks in connect-oriented flows.
        if table_name == "connect_settings" and not _contains_any_term(text, ("billing", "invoice", "payment")):
            score += 4.0

        scored.append((table_name, score))

    if not scored:
        return []

    scored.sort(key=lambda item: item[1], reverse=True)
    limit = max(1, int(max_tables or 3))
    return [table_name for table_name, _ in scored[:limit]]


def _rank_connect_message_tables(user_query, runtime_tables, max_tables=3):
    """Rank connect message tables using query cues to avoid wrong-table routing."""
    text = normalize_intent_text(user_query)
    if not text:
        return []

    runtime_set = {str(name or "").strip().lower() for name in (runtime_tables or [])}
    if not runtime_set:
        return []

    connect_candidates = [
        "connect_master_messages",
        "connect_individual_messages",
        "connect_chat_messages",
        "connect_automated_messages",
        "connect_test_messages",
        "connect_mailqueue",
    ]

    has_queue_hint = _contains_any_term(text, CONNECT_QUEUE_HINT_TERMS)
    has_master_hint = "master" in text
    has_message_hint = _contains_any_term(text, ("message", "messages"))
    has_automated_hint = "automated" in text
    has_individual_hint = "individual" in text
    has_chat_hint = "chat" in text
    has_test_hint = "test" in text
    has_sent_hint = _contains_any_term(text, CONNECT_SENT_TERMS)

    scored = []
    for table_name in connect_candidates:
        if table_name not in runtime_set:
            continue

        score = 0.0
        if table_name == "connect_master_messages":
            score += 30.0
        elif table_name == "connect_mailqueue":
            score += 6.0
        else:
            score += 12.0

        if has_message_hint and "message" in table_name:
            score += 14.0
        if has_sent_hint and "message" in table_name:
            score += 6.0
        if has_master_hint and "master" in table_name:
            score += 22.0
        if has_automated_hint and "automated" in table_name:
            score += 18.0
        if has_individual_hint and "individual" in table_name:
            score += 18.0
        if has_chat_hint and "chat" in table_name:
            score += 18.0
        if has_test_hint and "test" in table_name:
            score += 18.0
        if has_queue_hint and "mailqueue" in table_name:
            score += 24.0

        # Prevent queue table from winning generic "connect messages" prompts.
        if "mailqueue" in table_name and not has_queue_hint and has_message_hint:
            score -= 18.0

        scored.append((table_name, score))

    if not scored:
        return []

    scored.sort(key=lambda item: item[1], reverse=True)
    limit = max(1, int(max_tables or 3))
    return [table_name for table_name, _ in scored[:limit]]


def _is_ocr_request(user_query):
    """Return True when query explicitly asks about OCR/scan success details."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    ocr_terms = (
        "ocr",
        "optical character",
        "scan",
        "receipt scan",
        "scan success",
    )
    return any(term in text for term in ocr_terms)


def is_ocr_success_breakdown_request(user_query):
    """Return True when user asks OCR success/failure rate or percentage breakdown."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    if "ocr" not in text:
        return False

    success_or_match = any(
        token in text
        for token in (
            "success",
            "succes",
            "match",
            "matched",
        )
    )
    rate_or_breakdown = any(
        token in text
        for token in (
            "rate",
            "percentage",
            "percent",
            "ratio",
            "breakdown",
            "status",
        )
    )
    return success_or_match and rate_or_breakdown


def is_ocr_triggered_breakdown_request(user_query):
    """Return True when user asks OCR triggered vs not-triggered totals/percentages."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    if "ocr" not in text:
        return False

    has_triggered_term = any(
        (
            token in text
            if " " in token
            else re.search(rf"\b{re.escape(token)}\b", text) is not None
        )
        for token in (
            "triggered",
            "trigger",
            "not triggered",
            "untriggered",
        )
    )
    if not has_triggered_term:
        return False

    has_metric_term = any(
        (
            token in text
            if " " in token
            else re.search(rf"\b{re.escape(token)}\b", text) is not None
        )
        for token in (
            "percentage",
            "percent",
            "ratio",
            "breakdown",
            "count",
            "total",
            "totals",
            "how many",
        )
    )
    if not has_metric_term:
        return False

    # If user explicitly asks for row listing, do not route to percentage breakdown.
    listing_terms = (
        "list",
        "records",
        "record",
        "details",
        "data",
        "rows",
    )
    if any(re.search(rf"\b{re.escape(token)}\b", text) is not None for token in listing_terms):
        return False

    return True


def detect_ocr_triggered_records_scope(user_query):
    """Detect whether OCR row listing asks for triggered, not_triggered, or all records."""
    text = normalize_intent_text(user_query)
    if not text or "ocr" not in text:
        return "none"

    listing_terms = (
        "list",
        "records",
        "record",
        "details",
        "data",
        "rows",
        "show",
        "give",
        "fetch",
        "provide",
    )
    asks_listing = any(
        re.search(rf"\b{re.escape(token)}\b", text) is not None
        for token in listing_terms
    )
    if not asks_listing:
        return "none"

    has_not_triggered = (
        "not triggered" in text
        or re.search(r"\buntriggered\b", text) is not None
        or re.search(r"\bnot\s+trigger\w*\b", text) is not None
    )
    has_triggered = re.search(r"\btrigger\w*\b", text) is not None and not has_not_triggered

    if has_not_triggered:
        return "not_triggered"
    if has_triggered:
        return "triggered"
    return "all"


def build_ocr_triggered_records_sql(account_id, trigger_scope="all", row_limit=100):
    """Build deterministic SQL for OCR triggered/not-triggered record listing."""
    account_id_lit = sql_literal(account_id)
    safe_limit = max(1, min(int(row_limit), 2000)) if row_limit is not None else 100
    where_conditions = [f"account_id = {account_id_lit}"]

    if trigger_scope == "triggered":
        where_conditions.append("CAST(COALESCE(ocr_triggered, 0) AS UNSIGNED) = 1")
    elif trigger_scope == "not_triggered":
        where_conditions.append("CAST(COALESCE(ocr_triggered, 0) AS UNSIGNED) = 0")

    where_sql = " AND ".join(where_conditions)
    return (
        "SELECT * "
        "FROM track_packages "
        f"WHERE {where_sql} "
        "ORDER BY date_received DESC, package_id DESC "
        f"LIMIT {safe_limit}"
    )


def build_ocr_triggered_breakdown_sql(account_id):
    """Build deterministic SQL for OCR triggered vs not-triggered counts and percentages."""
    account_id_lit = sql_literal(account_id)
    return (
        "SELECT "
        "s.trigger_status, "
        "s.trigger_label, "
        "COALESCE(a.trigger_count, 0) AS trigger_count, "
        "ROUND(COALESCE(a.trigger_count, 0) * 100.0 / NULLIF(t.total_count, 0), 2) AS trigger_percentage "
        "FROM ("
        "SELECT 1 AS trigger_status, 'triggered' AS trigger_label "
        "UNION ALL SELECT 0, 'not_triggered'"
        ") s "
        "LEFT JOIN ("
        "SELECT "
        "CASE WHEN CAST(COALESCE(ocr_triggered, 0) AS UNSIGNED) = 1 THEN 1 ELSE 0 END AS trigger_status, "
        "COUNT(*) AS trigger_count "
        "FROM track_packages "
        f"WHERE account_id = {account_id_lit} "
        "GROUP BY trigger_status"
        ") a ON a.trigger_status = s.trigger_status "
        "CROSS JOIN ("
        "SELECT COUNT(*) AS total_count "
        "FROM track_packages "
        f"WHERE account_id = {account_id_lit}"
        ") t "
        "ORDER BY s.trigger_status DESC"
    )


def build_ocr_triggered_breakdown_answer(rows):
    """Build deterministic summary text for OCR triggered vs not-triggered metrics."""
    if not isinstance(rows, list) or not rows:
        return "No OCR-trigger data was found for this account."

    triggered_count = 0
    not_triggered_count = 0
    triggered_pct = 0.0
    not_triggered_pct = 0.0

    for row in rows:
        if not isinstance(row, dict):
            continue
        label = str(row.get("trigger_label", "")).strip().lower()
        count_value = _try_parse_float(row.get("trigger_count"))
        pct_value = _try_parse_float(row.get("trigger_percentage"))

        if label == "triggered":
            triggered_count = int(count_value) if count_value is not None else 0
            triggered_pct = float(pct_value) if pct_value is not None else 0.0
        elif label == "not_triggered":
            not_triggered_count = int(count_value) if count_value is not None else 0
            not_triggered_pct = float(pct_value) if pct_value is not None else 0.0

    total_count = triggered_count + not_triggered_count
    return (
        "OCR trigger summary for this account: "
        f"Triggered = {triggered_count} ({triggered_pct:.2f}%), "
        f"Not triggered = {not_triggered_count} ({not_triggered_pct:.2f}%), "
        f"Total = {total_count}."
    )


def build_ocr_success_breakdown_sql(account_id):
    """Build deterministic SQL for OCR 1-1 success and failure scenario percentages."""
    account_id_lit = sql_literal(account_id)
    return (
        "SELECT "
        "s.scenario_code, "
        "s.scenario_label, "
        "s.scenario_group, "
        "COALESCE(a.scenario_count, 0) AS scenario_count, "
        "ROUND(COALESCE(a.scenario_count, 0) * 100.0 / NULLIF(t.total_count, 0), 2) AS scenario_percentage "
        "FROM ("
        "SELECT '1-1' AS scenario_code, 'Success (tracking=1, recipient=1)' AS scenario_label, 'success' AS scenario_group "
        "UNION ALL SELECT '1-0', 'Failed (tracking=1, recipient=0)', 'failed' "
        "UNION ALL SELECT '0-1', 'Failed (tracking=0, recipient=1)', 'failed' "
        "UNION ALL SELECT '0-0', 'Failed (tracking=0, recipient=0)', 'failed'"
        ") s "
        "LEFT JOIN ("
        "SELECT "
        "CASE "
        "WHEN CAST(COALESCE(tracking_number_match_status, 0) AS UNSIGNED) = 1 "
        "AND CAST(COALESCE(recipient_match_status, 0) AS UNSIGNED) = 1 THEN '1-1' "
        "WHEN CAST(COALESCE(tracking_number_match_status, 0) AS UNSIGNED) = 1 "
        "AND CAST(COALESCE(recipient_match_status, 0) AS UNSIGNED) = 0 THEN '1-0' "
        "WHEN CAST(COALESCE(tracking_number_match_status, 0) AS UNSIGNED) = 0 "
        "AND CAST(COALESCE(recipient_match_status, 0) AS UNSIGNED) = 1 THEN '0-1' "
        "ELSE '0-0' "
        "END AS scenario_code, "
        "COUNT(*) AS scenario_count "
        "FROM track_ocr "
        f"WHERE account_id = {account_id_lit} "
        "GROUP BY scenario_code"
        ") a ON a.scenario_code = s.scenario_code "
        "CROSS JOIN ("
        "SELECT COUNT(*) AS total_count "
        "FROM track_ocr "
        f"WHERE account_id = {account_id_lit}"
        ") t "
        "ORDER BY FIELD(s.scenario_code, '1-1', '1-0', '0-1', '0-0')"
    )


def build_ocr_success_breakdown_answer(rows):
    """Build deterministic summary text for OCR success/fail scenario percentages."""
    if not isinstance(rows, list) or not rows:
        return "No OCR records were found for this account."

    scenario_counts = {"1-1": 0, "1-0": 0, "0-1": 0, "0-0": 0}
    scenario_percentages = {"1-1": 0.0, "1-0": 0.0, "0-1": 0.0, "0-0": 0.0}

    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("scenario_code", "")).strip()
        if code not in scenario_counts:
            continue

        count_value = _try_parse_float(row.get("scenario_count"))
        pct_value = _try_parse_float(row.get("scenario_percentage"))
        scenario_counts[code] = int(count_value) if count_value is not None else 0
        scenario_percentages[code] = float(pct_value) if pct_value is not None else 0.0

    success_count = scenario_counts["1-1"]
    total_count = sum(scenario_counts.values())
    failed_count = max(0, total_count - success_count)

    success_pct = scenario_percentages["1-1"]
    failed_pct = max(0.0, 100.0 - success_pct) if total_count > 0 else 0.0

    return (
        "OCR match summary for this account: "
        f"Success 1-1 = {success_count} ({success_pct:.2f}%), "
        f"Failed total (1-0, 0-1, 0-0) = {failed_count} ({failed_pct:.2f}%). "
        f"Scenario split: 1-0 = {scenario_counts['1-0']} ({scenario_percentages['1-0']:.2f}%), "
        f"0-1 = {scenario_counts['0-1']} ({scenario_percentages['0-1']:.2f}%), "
        f"0-0 = {scenario_counts['0-0']} ({scenario_percentages['0-0']:.2f}%)."
    )


def _get_expected_tables_for_strong_intent(user_query):
    """Infer strict expected table set for high-confidence query intents."""
    expected_tables = set()
    if _is_ocr_request(user_query):
        expected_tables.update(
            {
                "track_ocr",
                "track_receipt_scan_batches",
                "track_receipt_scan_items",
                "track_receipt_scan_pictures",
                # OCR flags may also be surfaced from package records.
                "track_packages",
            }
        )

    if is_account_billing_request(user_query):
        expected_tables.add("core_account_billing")

    return expected_tables


def validate_query_table_relevance(user_query, sql_query):
    """Validate strong intent and referenced SQL tables are semantically aligned."""
    if _is_connect_message_request(user_query):
        referenced_tables = set(_extract_referenced_tables(sql_query))
        if not referenced_tables:
            return False, "missing_table_reference"
        if any(str(table_name).startswith("connect_") for table_name in referenced_tables):
            return True, "ok"
        return False, "expected_prefix=connect_|referenced=" + ",".join(sorted(referenced_tables))
    expected_tables = _get_expected_tables_for_strong_intent(user_query)
    if not expected_tables:
        return True, "no_strong_expectation"

    referenced_tables = set(_extract_referenced_tables(sql_query))
    if not referenced_tables:
        return False, "missing_table_reference"

    if referenced_tables.intersection(expected_tables):
        return True, "ok"

    return False, (
        "expected="
        + ",".join(sorted(expected_tables))
        + "|referenced="
        + ",".join(sorted(referenced_tables))
    )


def validate_explicit_table_alignment(user_query, sql_query, schema_map):
    """Ensure explicit table-name asks are executed against the same table(s)."""
    explicit_tables = set(detect_explicit_query_tables(user_query, schema_map, max_tables=3))
    if not explicit_tables:
        return True, "no_explicit_table", []

    referenced_tables = set(_extract_referenced_tables(sql_query))
    if not referenced_tables:
        return False, "missing_table_reference", sorted(explicit_tables)

    if explicit_tables.intersection(referenced_tables):
        return True, "ok", sorted(explicit_tables)

    return (
        False,
        "explicit_table_mismatch:expected="
        + ",".join(sorted(explicit_tables))
        + "|referenced="
        + ",".join(sorted(referenced_tables)),
        sorted(explicit_tables),
    )


def build_explicit_table_no_data_response(explicit_tables):
    """Deterministic no-data response for explicit table queries."""
    if not explicit_tables:
        return build_no_data_fallback_from_query("")
    first_table = str(explicit_tables[0]).strip()
    if not first_table:
        return build_no_data_fallback_from_query("")
    return f"No matching data was found in {first_table} for this account."


def build_account_billing_sql(account_id, user_query, row_limit=20):
    """Build deterministic SQL for account billing details in account scope."""
    text = normalize_intent_text(user_query)
    account_id_lit = sql_literal(account_id)
    safe_limit = None if row_limit is None else max(1, min(int(row_limit), 100))

    # Return a concise, relevant subset of billing columns for chatbot usage.
    selected_columns = [
        "account_id",
        "billing_method",
        "billing_interval",
        "cc_number",
        "cc_type",
        "cc_name",
        "cc_month",
        "cc_year",
        "bill_email",
        "bill_address1",
        "bill_city",
        "bill_state",
        "bill_zipcode",
        "payment_terms",
        "currency_code",
        "paypal_payment_id",
        "braintree_customer_id",
    ]

    where_conditions = [f"account_id = {account_id_lit}"]

    # Optional fuzzy holder-name filter for prompts like "what is syta saephan cc number".
    holder_match = re.search(
        r"\b(?:what\s+is|show|give|tell\s+me)?\s*([a-z][a-z\s]{2,60}?)\s+(?:cc\s*number|credit\s*card|card\s*number)\b",
        text,
    )
    if holder_match:
        holder_phrase = re.sub(r"\s+", " ", holder_match.group(1)).strip()
        if holder_phrase and holder_phrase not in {"my", "the", "this", "that", "account"}:
            where_conditions.append(
                f"LOWER(COALESCE(cc_name, '')) LIKE {sql_literal('%' + holder_phrase.lower() + '%')}"
            )

    sql_text = (
        f"SELECT {', '.join(selected_columns)} "
        "FROM core_account_billing "
        f"WHERE {' AND '.join(where_conditions)}"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def build_carrier_count_sql(
    account_id,
    user_query,
    row_limit=10,
    start_date=None,
    end_date=None,
    override_time_filters=False,
):
    """Build deterministic carrier-wise package count SQL in account scope."""
    text = normalize_intent_text(user_query)
    account_id_lit = sql_literal(account_id)
    safe_limit = None if row_limit is None else max(1, min(int(row_limit), 100))
    sort_direction = "ASC" if is_low_package_count_request(text) else "DESC"

    # Peak-carrier prompts (maximum/highest/lowest) without explicit "top N"
    # should return the single peak carrier by default for non-chart asks.
    # Chart/plural-carrier prompts should keep grouped multi-row output.
    has_peak_only_intent = any(term in text for term in ("maximum", "highest", "minimum", "lowest", "least", "max", "min"))
    explicit_rank_limit = (
        extract_ranked_limit_from_raw_query(user_query, max_limit=100)
        or extract_requested_row_limit(user_query, max_limit=100)
    )
    chart_or_visual_context = query_requests_chart_view(text) or any(
        term in text
        for term in (
            "visualize",
            "visualise",
            "visulaise",
            "compare",
            "distribution",
            "breakdown",
            "graph",
            "plot",
            "chart",
        )
    )
    plural_carrier_context = any(term in text for term in ("carriers", "each carrier", "by carrier"))
    if has_peak_only_intent and explicit_rank_limit is None and not (chart_or_visual_context or plural_carrier_context):
        safe_limit = 1

    conditions = [f"account_id = {account_id_lit}"]
    if any(token in text for token in RECIPIENT_WISE_DELIVERED_TERMS):
        conditions.append("date_received IS NOT NULL")
    explicit_date_condition = build_explicit_date_range_condition("date_received", start_date, end_date)
    if override_time_filters and explicit_date_condition:
        conditions.append(explicit_date_condition)
    else:
        if any(token in text for token in RECIPIENT_WISE_TODAY_TERMS):
            conditions.append("DATE(date_received) = CURDATE()")
        elif "yesterday" in text:
            conditions.append("DATE(date_received) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        else:
            relative_window_condition = build_relative_time_window_condition(text, "date_received")
            if relative_window_condition:
                conditions.append(relative_window_condition)
        requested_year = extract_requested_calendar_year(text)
        if requested_year is not None:
            conditions.append(f"YEAR(date_received) = {requested_year}")
        if explicit_date_condition:
            conditions.append(explicit_date_condition)

    where_sql = " AND ".join(conditions)
    sql_text = (
        "SELECT shipping_carrier, COUNT(*) AS package_count "
        "FROM track_packages "
        f"WHERE {where_sql} "
        "GROUP BY shipping_carrier "
        "HAVING COALESCE(shipping_carrier, '') <> '' "
        f"ORDER BY package_count {sort_direction}"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def build_carrier_percentage_sql(
    account_id,
    user_query,
    row_limit=None,
    start_date=None,
    end_date=None,
    override_time_filters=False,
):
    """Build deterministic carrier-wise percentage SQL using full scoped total."""
    text = normalize_intent_text(user_query)
    account_id_lit = sql_literal(account_id)
    safe_limit = None if row_limit is None else max(1, min(int(row_limit), 100))

    conditions = [f"account_id = {account_id_lit}"]
    if any(token in text for token in RECIPIENT_WISE_DELIVERED_TERMS):
        conditions.append("date_received IS NOT NULL")
    explicit_date_condition = build_explicit_date_range_condition("date_received", start_date, end_date)
    if override_time_filters and explicit_date_condition:
        conditions.append(explicit_date_condition)
    else:
        if any(token in text for token in RECIPIENT_WISE_TODAY_TERMS):
            conditions.append("DATE(date_received) = CURDATE()")
        elif "yesterday" in text:
            conditions.append("DATE(date_received) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        else:
            relative_window_condition = build_relative_time_window_condition(text, "date_received")
            if relative_window_condition:
                conditions.append(relative_window_condition)
        requested_year = extract_requested_calendar_year(text)
        if requested_year is not None:
            conditions.append(f"YEAR(date_received) = {requested_year}")
        if explicit_date_condition:
            conditions.append(explicit_date_condition)

    where_sql = " AND ".join(conditions)
    sql_text = (
        "SELECT shipping_carrier, "
        "COUNT(*) AS package_count, "
        "ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS percentage "
        "FROM track_packages "
        f"WHERE {where_sql} "
        "GROUP BY shipping_carrier "
        "HAVING COALESCE(shipping_carrier, '') <> '' "
        "ORDER BY percentage DESC, shipping_carrier ASC"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def build_recipient_count_sql(
    account_id,
    user_query,
    row_limit=100,
    start_date=None,
    end_date=None,
    override_time_filters=False,
):
    """Build deterministic recipient-wise package count SQL using package counts + recipient directory."""
    text = normalize_intent_text(user_query)
    account_id_lit = sql_literal(account_id)
    safe_limit = None if row_limit is None else max(1, min(int(row_limit), 100))
    sort_direction = "ASC" if is_low_package_count_request(text) else "DESC"

    alias_name = re.sub(r"[^a-z0-9_]", "", RECIPIENT_WISE_COUNT_ALIAS) or "package_count"

    if RECIPIENT_WISE_COUNT_METRIC_COLUMN == "*":
        count_expression = "COUNT(*)"
    else:
        metric_column = _safe_identifier(
            RECIPIENT_WISE_COUNT_METRIC_COLUMN,
            TRACK_PACKAGES_COLUMNS,
            "package_id",
        )
        count_expression = f"COUNT(tp.{metric_column})"

    conditions = [f"tp.account_id = {account_id_lit}"]
    if any(token in text for token in RECIPIENT_WISE_DELIVERED_TERMS):
        conditions.append("tp.date_received IS NOT NULL")
    explicit_date_condition = build_explicit_date_range_condition("tp.date_received", start_date, end_date)
    if override_time_filters and explicit_date_condition:
        conditions.append(explicit_date_condition)
    else:
        if any(token in text for token in RECIPIENT_WISE_TODAY_TERMS):
            conditions.append("DATE(tp.date_received) = CURDATE()")
        elif "yesterday" in text:
            conditions.append("DATE(tp.date_received) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
        else:
            relative_window_condition = build_relative_time_window_condition(text, "tp.date_received")
            if relative_window_condition:
                conditions.append(relative_window_condition)
        requested_year = extract_requested_calendar_year(text)
        if requested_year is not None:
            conditions.append(f"YEAR(tp.date_received) = {requested_year}")
        if explicit_date_condition:
            conditions.append(explicit_date_condition)

    where_sql = " AND ".join(conditions)
    sql_text = (
        "SELECT "
        "COALESCE(NULLIF(TRIM(CONCAT_WS(' ', NULLIF(COALESCE(cr.preferred_first_name, cr.first_name), ''), NULLIF(cr.last_name, ''))), ''), "
        "NULLIF(TRIM(tp.recipient_name), ''), NULLIF(cr.email, ''), CONCAT('Recipient ', tp.recipient_id)) AS recipient_name, "
        "tp.recipient_id, "
        f"{count_expression} AS {alias_name} "
        "FROM track_packages tp "
        "LEFT JOIN core_recipients cr "
        "ON tp.recipient_id = cr.recipient_id AND tp.account_id = cr.account_id "
        f"WHERE {where_sql} "
        "GROUP BY tp.recipient_id, recipient_name "
        f"ORDER BY {alias_name} {sort_direction}, recipient_name ASC"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def build_recipient_wise_count_sql(account_id, user_query, row_limit=100):
    """Build deterministic recipient-wise package count SQL in account scope."""
    return build_recipient_count_sql(account_id, user_query, row_limit=row_limit)


def analyze_delivery_date_intent(user_query, last_user_query=""):
    """Dynamically detect delivered-date analytics intent from natural language."""
    current = normalize_intent_text(user_query)
    previous = normalize_intent_text(last_user_query)

    if not current:
        return {"mode": "none", "normalized_query": user_query}

    has_date_token = any(phrase in current for phrase in DATE_TOKEN_TERMS)
    has_delivered_token = "delivered" in current

    is_short_followup = len(current.split()) <= 4 and has_date_token
    if is_short_followup and "delivered" in previous:
        current = f"delivered date details for {previous}"
        has_delivered_token = True

    if not (has_date_token and has_delivered_token):
        return {"mode": "none", "normalized_query": current}

    asks_peak = any(phrase in current for phrase in PEAK_TERMS)

    asks_latest = any(phrase in current for phrase in LATEST_DATE_TERMS)

    if asks_latest:
        mode = "latest_date"
    else:
        mode = "peak" if asks_peak else "dates"
    return {"mode": mode, "normalized_query": current}


def build_peak_delivered_date_sql(account_id):
    """Build deterministic SQL for highest delivered package date in account."""
    account_id_lit = sql_literal(account_id)
    return (
        "SELECT DATE(date_received) AS delivered_date, COUNT(*) AS delivered_count "
        "FROM track_packages "
        f"WHERE account_id = {account_id_lit} AND date_received IS NOT NULL "
        "GROUP BY DATE(date_received) "
        "ORDER BY delivered_count DESC, delivered_date DESC "
        "LIMIT 1"
    )


def build_delivered_dates_sql(account_id, limit=20):
    """Build deterministic SQL for delivered date list in account scope."""
    account_id_lit = sql_literal(account_id)
    safe_limit = None if limit is None else max(1, min(int(limit), 100))
    sql_text = (
        "SELECT DATE(date_received) AS delivered_date "
        "FROM track_packages "
        f"WHERE account_id = {account_id_lit} AND date_received IS NOT NULL "
        "GROUP BY DATE(date_received) "
        "ORDER BY delivered_date DESC"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def _safe_identifier(candidate, allowed_columns, fallback):
    """Return safe SQL identifier if present in allowed columns, else fallback."""
    candidate_text = str(candidate or "").strip().lower()
    allowed = {str(col).strip().lower() for col in allowed_columns}
    if candidate_text in allowed:
        return candidate_text
    return fallback


def build_latest_packages_sql(account_id, row_limit=10, schema_columns=None, relative_time_window=None):
    """Build deterministic SQL for latest packages ordered by configurable date column."""
    account_id_lit = sql_literal(account_id)
    safe_limit = None if row_limit is None else max(1, min(int(row_limit), 100))
    columns = schema_columns or TRACK_PACKAGES_COLUMNS
    date_col = _safe_identifier(
        os.getenv("CHATBOT_LATEST_PACKAGE_DATE_COLUMN", "date_received"),
        columns,
        "date_received",
    )
    tie_breaker_col = _safe_identifier(
        os.getenv("CHATBOT_LATEST_PACKAGE_TIE_BREAKER_COLUMN", "package_id"),
        columns,
        "package_id",
    )

    conditions = [f"account_id = {account_id_lit}", f"{date_col} IS NOT NULL"]
    if relative_time_window:
        window_value = max(1, min(int(relative_time_window.get("value", 1)), 120))
        window_unit = str(relative_time_window.get("unit", "DAY")).upper()
        if window_unit not in {"DAY", "WEEK", "MONTH", "YEAR"}:
            window_unit = "DAY"
        relative_condition = build_relative_time_window_condition(
            f"last {window_value} {window_unit.lower()}",
            date_col,
        )
        if relative_condition:
            conditions.append(relative_condition)

    where_sql = " AND ".join(conditions)
    sql_text = (
        "SELECT * FROM track_packages "
        f"WHERE {where_sql} "
        f"ORDER BY {date_col} DESC, {tie_breaker_col} DESC"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def format_date_to_mmddyyyy(value):
    """Format DB date/datetime values to MM/DD/YYYY when parseable."""
    if value is None:
        return ""

    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")

    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")

    text = str(value).strip()
    if not text:
        return ""

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.strftime("%m/%d/%Y")
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(text[:10], fmt)
            return parsed.strftime("%m/%d/%Y")
        except ValueError:
            continue

    return text


def extract_delivered_date_value(row):
    """Extract delivered date value from varying SQL/driver column aliases."""
    if not isinstance(row, dict):
        return None
    return get_first_available_value(
        row,
        (
            "delivered_date",
            "date(date_received)",
            "date_received",
            "date(date_pickedup)",
            "date_pickedup",
        ),
    )


def get_first_available_value(row, candidates):
    """Get first non-empty value from candidate keys in a row dict."""
    if not row:
        return None
    lowered = {str(k).lower(): v for k, v in row.items()}
    for key in candidates:
        if key in lowered and lowered[key] not in (None, ""):
            return lowered[key]
    return None


def build_explicit_answer(user_query, rows, generated_sql=""):
    """Return precise natural-language answer for explicit data questions."""
    intent = detect_explicit_data_intent(user_query)
    query_text = normalize_intent_text(user_query)
    normalized_sql = str(generated_sql or "").lower()

    asks_top_recipient = is_top_recipient_request(query_text) or (
        "group by recipient_name" in normalized_sql
        and "order by" in normalized_sql
        and "count(" in normalized_sql
    )
    asks_top_carrier = is_top_carrier_request(query_text) or (
        "group by shipping_carrier" in normalized_sql
        and "order by" in normalized_sql
        and "count(" in normalized_sql
    )
    asks_low_recipient = is_low_package_count_request(query_text) or (
        "order by" in normalized_sql and " asc" in normalized_sql and "count(" in normalized_sql
    )

    if asks_top_recipient:
        if not rows:
            return "No matching organizational data was found for this request."

        first_row = rows[0]
        recipient = get_first_available_value(
            first_row,
            (
                "recipient_name",
                "concat(recipient_name)",
                "name",
            ),
        )
        package_count = get_first_available_value(
            first_row,
            (
                "package_count",
                "count",
                "total_records",
                "record_count",
                "pkg_count",
                "num_packages",
            ),
        )

        if recipient is not None and package_count is not None:
            if asks_low_recipient:
                return f"{recipient} has the least logged packages with {package_count} package(s)."
            return f"{recipient} has the most logged packages with {package_count} package(s)."

        if package_count is not None:
            if asks_low_recipient:
                return f"The lowest logged package count is {package_count}."
            return f"The highest logged package count is {package_count}."

    if asks_top_carrier:
        if not rows:
            return "No matching organizational data was found for this request."

        first_row = rows[0]
        carrier_name = get_first_available_value(
            first_row,
            (
                "shipping_carrier",
                "carrier",
                "service_provider",
            ),
        )
        package_count = get_first_available_value(
            first_row,
            (
                "package_count",
                "count",
                "total_records",
                "record_count",
                "pkg_count",
                "num_packages",
            ),
        )

        if carrier_name is not None and package_count is not None:
            if asks_low_recipient:
                return f"{carrier_name} has the least delivered packages with {package_count} package(s)."
            return f"{carrier_name} has the most delivered packages with {package_count} package(s)."

    if intent == "recipient_wise_count":
        if not rows:
            return "No matching organizational data was found for this request."
        return f"Here is the recipient-wise package count ({len(rows)} recipient record(s))."

    if intent == "none":
        return None

    if intent == "count":
        if not rows:
            return "The total matching records are 0."
        count_value = get_first_available_value(
            rows[0],
            (
                "total_records",
                "count",
                "record_count",
                "package_count",
                "pkg_count",
                "num_packages",
            ),
        )
        if count_value is None:
            count_value = len(rows)
        return f"The total matching records are {count_value}."

    if not rows:
        return "No matching organizational data was found for this request."

    first_row = rows[0]
    if intent == "tracking_number":
        value = get_first_available_value(first_row, ("tracking_number", "tracking_no", "trackingid"))
        if value is not None:
            return f"The tracking number is {value}."

    if intent == "package_id":
        value = get_first_available_value(first_row, ("package_id", "packageid"))
        if value is not None:
            return f"The package id is {value}."

    if intent == "shipping_carrier":
        value = get_first_available_value(first_row, ("shipping_carrier", "carrier", "service_provider"))
        if value is not None:
            return f"The shipping carrier is {value}."

    if intent == "status":
        value = get_first_available_value(first_row, ("status", "package_status", "delivery_status"))
        if value is not None:
            return f"The current status is {value}."

    return None


def build_yearly_package_count_answer(rows):
    """Build a deterministic narrative for year-wise package counts."""
    if not isinstance(rows, list) or not rows:
        return "No matching organizational data was found for this request."

    year_count_pairs = []
    total_packages = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        year_value = get_first_available_value(row, ("delivery_year", "year"))
        count_value = _try_parse_float(get_first_available_value(row, ("package_count", "count", "total_records")))
        if year_value in (None, "") or count_value is None:
            continue

        year_label = str(year_value).strip()
        if not year_label:
            continue

        if float(count_value).is_integer():
            count_value = int(count_value)
        else:
            count_value = round(count_value, 2)

        year_count_pairs.append((year_label, count_value))
        total_packages += int(count_value) if isinstance(count_value, int) else count_value

    if not year_count_pairs:
        return "No matching organizational data was found for this request."

    year_count_pairs.sort(key=lambda item: item[0])
    per_year_text = ", ".join(f"{year}: {count}" for year, count in year_count_pairs)
    return f"Year-wise package counts are {per_year_text}. Total across selected years: {total_packages}."


def build_monthly_delivered_count_answer(rows):
    """Build a deterministic narrative for month-wise delivered package counts."""
    if not isinstance(rows, list) or not rows:
        return "No matching organizational data was found for this request."

    month_count_pairs = []
    total_packages = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        month_value = get_first_available_value(row, ("delivery_month", "month"))
        count_value = _try_parse_float(get_first_available_value(row, ("package_count", "count", "total_records")))
        if month_value in (None, "") or count_value is None:
            continue

        month_label = str(month_value).strip()
        if not month_label:
            continue

        if float(count_value).is_integer():
            count_value = int(count_value)
        else:
            count_value = round(count_value, 2)

        month_count_pairs.append((month_label, count_value))
        total_packages += int(count_value) if isinstance(count_value, int) else count_value

    if not month_count_pairs:
        return "No matching organizational data was found for this request."

    month_count_pairs.sort(key=lambda item: item[0])
    per_month_text = ", ".join(f"{month}: {count}" for month, count in month_count_pairs)
    return f"Month-wise delivered package counts are {per_month_text}. Total across selected months: {total_packages}."


def _row_value(row, keys):
    """Return first present value in row for candidate keys."""
    if not isinstance(row, dict):
        return ""
    lowered = {str(k).lower(): v for k, v in row.items()}
    for key in keys:
        if key in lowered and lowered[key] is not None:
            return str(lowered[key])
    return ""


def filter_session_rows_by_query(rows, user_query):
    """Apply simple in-memory filters from user query to session rows."""
    text = normalize_intent_text(user_query)
    filtered = list(rows)
    has_carrier_context = any(keyword in text for aliases in CARRIER_FILTERS.values() for keyword in aliases)

    for _, aliases in CARRIER_FILTERS.items():
        if any(alias in text for alias in aliases):
            filtered = [
                row
                for row in filtered
                if any(
                    alias in _row_value(row, ("shipping_carrier", "carrier", "service_provider")).lower()
                    for alias in aliases
                )
            ]
            break

    if "delivered" in text:
        delivered_filtered = [
            row
            for row in filtered
            if (
                "delivered" in _row_value(row, ("status", "package_status", "delivery_status")).lower()
                or _row_value(row, ("date_pickedup",)).strip().lower()
                not in EMPTY_DATE_VALUES
                or _row_value(row, ("date_received",)).strip().lower()
                not in EMPTY_DATE_VALUES
            )
        ]
        # For phrases like "delivered through usps", user intent is often carrier-focused.
        # Keep carrier-matched rows if strict status filtering removes everything.
        if delivered_filtered:
            filtered = delivered_filtered
        elif not has_carrier_context:
            filtered = delivered_filtered

    if "today" in text:
        today_str = datetime.now().strftime("%Y-%m-%d")
        filtered = [
            row
            for row in filtered
            if _row_value(row, ("date_received", "created_at", "created_on", "updated_at")).startswith(today_str)
        ]

    # Generic phrase filter for requests like "through X", "via X", "to X" when X is not a known carrier keyword.
    stop_terms = PHRASE_STOP_TERMS
    phrase_match = re.search(PHRASE_CONNECTOR_PATTERN, text)
    if phrase_match:
        phrase = phrase_match.group(1).strip()
        # Keep only meaningful tokens; limit to first 3 for predictable filtering.
        tokens = [tok for tok in phrase.split() if tok not in stop_terms][:3]
        if tokens:
            search_keys = (
                "shipping_carrier",
                "carrier",
                "service_provider",
                "destination",
                "destination_city",
                "destination_state",
                "state",
                "city",
                "address",
                "recipient",
            )

            def row_matches_tokens(row):
                haystack = " ".join(_row_value(row, (key,)).lower() for key in search_keys)
                return all(token in haystack for token in tokens)

            filtered = [row for row in filtered if row_matches_tokens(row)]

    return filtered


def build_no_data_fallback_from_query(user_query):
    """Deterministic fallback for no-data scenarios when LLM is unavailable."""
    text = normalize_intent_text(user_query)
    no_data_templates = _load_json_env(
        "CHATBOT_NO_DATA_TEMPLATES",
        {
            "amazon_delivered": "No packages were delivered through Amazon for this account.",
            "usps_delivered": "No packages were delivered through USPS for this account.",
            "today": "No matching package records were found for this account today.",
            "default": "No matching package records were found for this account based on your request.",
        },
    )
    if "amazon" in text and "delivered" in text:
        return str(no_data_templates.get("amazon_delivered", OUT_OF_DB_RESPONSE))
    if ("usps" in text or "postal" in text) and "delivered" in text:
        return str(no_data_templates.get("usps_delivered", OUT_OF_DB_RESPONSE))
    if "today" in text:
        return str(no_data_templates.get("today", OUT_OF_DB_RESPONSE))
    return str(no_data_templates.get("default", OUT_OF_DB_RESPONSE))


def format_rows_for_chat(rows, max_rows=100):
    """Format rows in readable text for chatbot UI display."""
    limited_rows = rows[:max_rows]
    return json.dumps(limited_rows, indent=2, default=str)


def query_requests_table_view(user_query, requested_fields=None):
    """Return True when user asks for list/table or requests 2+ fields."""
    text = normalize_intent_text(user_query)
    explicit_table_request = any(term in text for term in TABLE_VIEW_TERMS) or any(
        re.search(pattern, text)
        for pattern in (
            r"\bshow(?:\s+me)?\b",
            r"\bgive(?:\s+me)?\b",
            r"\bprovide(?:\s+me)?\b",
            r"\bfetch(?:\s+me)?\b",
            r"\bdetail(?:s)?\b",
            r"\bdata\b",
        )
    )
    multi_field_request = isinstance(requested_fields, list) and len(requested_fields) >= 2
    return explicit_table_request or multi_field_request or is_recipient_wise_count_request(text)


def query_requests_chart_view(user_query):
    """Return True when user explicitly requests a chart/graph visualization."""
    text = normalize_intent_text(user_query)
    return any(re.search(pattern, text) for pattern in CHART_MODE_PATTERNS)


def query_requests_text_only_view(user_query):
    """Return True when user wants narrative response without table/chart."""
    text = normalize_intent_text(user_query)
    return any(re.search(pattern, text) for pattern in TEXT_MODE_PATTERNS)


def _is_status_distribution_query(user_query):
    """Return True for status percentage/distribution requests."""
    text = normalize_intent_text(user_query)
    if not text:
        return False
    status_intent = any(term in text for term in ("active", "inactive", "status"))
    distribution_intent = any(
        term in text
        for term in ("percentage", "percent", "share", "distribution", "breakdown", "ratio", "compare")
    )
    return status_intent and distribution_intent


def resolve_requested_operation(user_query, requested_fields=None):
    """Classify request operation dynamically to keep render behavior consistent."""
    text = normalize_intent_text(user_query)
    if not text:
        return {
            "mode": "auto",
            "explicit_chart": False,
            "explicit_table": False,
            "explicit_text": False,
            "chart_full_data": False,
        }

    explicit_text = query_requests_text_only_view(text)
    explicit_table = query_requests_table_view(text, requested_fields=requested_fields)
    explicit_chart = query_requests_chart_view(text)

    visual_force_terms = (
        "generate",
        "visualize",
        "visualise",
        "visulaise",
        "compare",
        "comparison",
    )
    force_graph_from_action = any(term in text for term in visual_force_terms)

    chart_action_terms = (
        "generate",
        "visualize",
        "visualise",
        "compare",
        "comparison",
        "trend",
        "distribution",
        "breakdown",
        "percentage",
        "percent",
        "share",
        "graph",
        "plot",
        "chart",
    )
    chart_subject_terms = (
        "recipient",
        "recipients",
        "carrier",
        "carriers",
        "shipping",
        "package",
        "packages",
        "status",
        "active",
        "inactive",
        "year",
        "years",
        "monthly",
        "daily",
        "date",
        "dates",
    )
    analytic_metric_terms = (
        "count",
        "total",
        "maximum",
        "minimum",
        "highest",
        "lowest",
        "top",
        "ratio",
    )

    has_chart_action = any(term in text for term in chart_action_terms)
    has_chart_subject = any(term in text for term in chart_subject_terms)
    has_metric_term = any(term in text for term in analytic_metric_terms)

    # Treat "generate/visualize/compare" analytics as chart intent when no text/table override exists.
    inferred_chart = has_chart_action and (has_chart_subject or has_metric_term)

    chart_requested = (explicit_chart or inferred_chart or force_graph_from_action or _is_status_distribution_query(text)) and not explicit_text

    if explicit_text:
        mode = "text"
    elif force_graph_from_action:
        mode = "chart"
    elif explicit_table:
        mode = "table"
    elif chart_requested:
        mode = "chart"
    else:
        mode = "auto"

    chart_full_data = chart_requested or force_graph_from_action

    return {
        "mode": mode,
        "explicit_chart": explicit_chart,
        "explicit_table": explicit_table,
        "explicit_text": explicit_text,
        "chart_full_data": chart_full_data,
    }


def should_fetch_full_data_for_chart_query(user_query):
    """Return True when query should use full dataset for chart rendering."""
    profile = resolve_requested_operation(user_query)
    return bool(profile.get("chart_full_data"))


def should_auto_chart_view(user_query, rows):
    """Return True when query/result shape suggests chart-first analytical rendering."""
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        return False

    text = normalize_intent_text(user_query)
    if not text:
        return False

    # Respect explicit user preference for text/table view.
    if query_requests_text_only_view(text) or query_requests_table_view(text):
        return False

    # Status distribution/percentage questions should chart even when row shape is wide.
    if _is_status_distribution_query(text):
        status_field = _find_status_field(rows[:CHART_MAX_POINTS])
        if status_field:
            return True

    analytic_terms = (
        "top",
        "maximum",
        "minimum",
        "highest",
        "lowest",
        "count",
        "total",
        "distribution",
        "percentage",
        "trend",
        "compare",
        "summary",
        "report",
    )
    if not any(term in text for term in analytic_terms):
        return False

    column_count = len(rows[0].keys())
    if column_count > 4:
        return False

    kinds = _infer_column_kinds(rows)
    has_numeric = any(kind == "numeric" for kind in kinds.values())
    has_date = any(kind == "date" for kind in kinds.values())
    has_category = any(kind == "category" for kind in kinds.values())
    if not has_numeric:
        return False

    return has_date or has_category


def detect_requested_chart_type(user_query):
    """Detect explicit chart type mentioned by user text."""
    text = normalize_intent_text(user_query)
    if not text:
        return ""

    percentage_intent = any(
        term in text
        for term in (
            "percentage",
            "percentages",
            "percent",
            "perecentage",
            "precentage",
            "share",
            "distribution",
            "breakdown",
            "ratio",
        )
    )
    visualization_intent = any(
        term in text
        for term in ("chart", "graph", "visual", "visualize", "plot", "show")
    )

    if re.search(r"\bpie\b", text) or re.search(r"\bpie\s+chart\b", text) or re.search(r"\bdoughnut\b", text) or re.search(r"\bdonut\b", text):
        return "pie"
    if percentage_intent and visualization_intent:
        return "pie"
    if (
        re.search(r"\bbar\b", text)
        or re.search(r"\bbar\s+chart\b", text)
        or re.search(r"\bvbar\b", text)
        or re.search(r"\bvertical\s+bar\b", text)
        or re.search(r"\bcolumn\s+chart\b", text)
        or re.search(r"\bcolumn\b", text)
    ):
        return "bar"
    if re.search(r"\bline\b", text) or re.search(r"\bline\s+chart\b", text):
        return "line"
    if re.search(r"\bscatter\b", text):
        return "scatter"
    if re.search(r"\bhistogram\b", text):
        return "histogram"
    return ""


def is_specific_chart_request(user_query):
    """Return True when user explicitly requests a concrete chart type."""
    return bool(detect_requested_chart_type(user_query))


def build_chart_unavailable_payload(user_query):
    """Return chart-only payload when requested chart type cannot be rendered."""
    requested_type = detect_requested_chart_type(user_query) or "chart"
    return {
        "chart_type": requested_type,
        "title": f"Requested {requested_type} chart",
        "x_field": "",
        "y_field": "",
        "point_count": 0,
        "image_base64": "",
        "labels": [],
        "values": [],
        "count_values": [],
        "percentage_values": [],
        "x_values": [],
        "y_values": [],
        "render_error": "requested_chart_unavailable",
    }


def is_ranking_or_count_query(user_query):
    """Return True when user asks ranking/count style analytical question."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    ranking_terms = (
        "top",
        "maximum",
        "minimum",
        "highest",
        "lowest",
        "count",
        "total",
        "most",
        "least",
    )
    has_ranking = any(term in text for term in ranking_terms)
    has_subject = any(term in text for term in ("recipient", "recipients", "package", "packages"))
    return has_ranking and has_subject


def _format_numeric_label(value):
    """Return compact readable numeric label for chart annotations."""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)

    if abs(numeric - int(numeric)) < 1e-9:
        return f"{int(numeric)}"
    return f"{numeric:.2f}"


def _compute_annotation_indices(total_points):
    """Pick evenly-spaced annotation indices based on point count."""
    total = int(total_points or 0)
    if total <= 0:
        return []

    label_budget = min(total, CHART_MAX_DATA_LABELS)
    if label_budget >= total:
        return list(range(total))

    if label_budget <= 1:
        return [0]

    step = (total - 1) / float(label_budget - 1)
    indices = []
    seen = set()
    for slot in range(label_budget):
        index = int(round(slot * step))
        index = max(0, min(total - 1, index))
        if index in seen:
            continue
        seen.add(index)
        indices.append(index)

    if indices[-1] != total - 1:
        indices.append(total - 1)

    return indices


def _should_use_pie_by_intent(user_query, labels):
    """Decide whether pie is suitable based on intent and category count."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    # Pie is most meaningful when asking for share/percentage style insight.
    percentage_intent = any(
        term in text
        for term in (
            "percentage",
            "percentages",
            "percent",
            "perecentage",
            "precentage",
            "share",
            "distribution",
            "breakdown",
            "ratio",
        )
    )
    if not percentage_intent:
        return False

    # Keep pie readable; threshold is configurable for real-world category counts.
    return 2 <= len(labels) <= CHART_MAX_PIE_CATEGORIES
def _has_trend_intent(user_query):
    """Return True when user asks for a time trend/series chart."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    trend_terms = (
        "trend",
        "over time",
        "daily",
        "weekly",
        "monthly",
        "yearly",
        "timeline",
        "by date",
        "over days",
        "over months",
    )
    return any(term in text for term in trend_terms)

def _find_status_field(rows):
    """Find likely status field from rows for status-based charts."""
    if not rows or not isinstance(rows[0], dict):
        return ""
    preferred = ("recipient_status", "status", "package_status", "delivery_status")
    row_keys = [str(key) for key in rows[0].keys()]
    lowered = {key.lower(): key for key in row_keys}
    for key in preferred:
        if key in lowered:
            return lowered[key]
    return ""

def _status_label_from_value(value):
    """Map raw recipient status values/codes to readable labels."""
    if value is None:
        return "Unknown"

    raw = str(value).strip()
    if not raw:
        return "Unknown"

    lowered = raw.lower()
    inverse_code_map = {}
    for label, codes in RECIPIENT_STATUS_CODE_MAP.items():
        for code in codes:
            inverse_code_map[str(code).strip().lower()] = str(label).strip().title()

    if lowered in inverse_code_map:
        return inverse_code_map[lowered]

    if lowered in ("active", "inactive", "future"):
        return lowered.title()

    return raw


def _choose_best_category_column(category_columns, user_query):
    """Pick the best category field for chart labels based on intent and availability."""
    if not category_columns:
        return ""

    lowered_columns = {str(column).lower(): str(column) for column in category_columns}

    # Prefer explicit recipient display name when available.
    if "recipient_name" in lowered_columns:
        return lowered_columns["recipient_name"]

    text = normalize_intent_text(user_query)
    recipient_intent = any(term in text for term in ("recipient", "recipients", "user", "users", "person", "people"))
    if recipient_intent:
        for candidate in ("name", "full_name", "preferred_first_name", "first_name", "last_name"):
            if candidate in lowered_columns:
                return lowered_columns[candidate]

    return category_columns[0]


def _try_parse_float(value):
    """Best-effort numeric parser for chart inference."""
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value:
        return None
    normalized = text_value.replace(",", "")
    try:
        return float(normalized)
    except ValueError:
        return None


def reshape_rows_for_chart(user_query, rows):
    """Dynamically reshape pivot-style rows into chart-friendly points when possible."""
    if not query_requests_chart_view(user_query):
        return rows

    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        return rows

    first_row = rows[0]
    normalized_keys = {str(key).lower() for key in first_row.keys()}
    if "delivery_year" in normalized_keys and "package_count" in normalized_keys:
        return rows

    # Detect columns such as packages_2025, total_2026, count_2027 and fold them into
    # rows shaped as {delivery_year, package_count} for reliable X/Y chart inference.
    year_key_pattern = re.compile(r"(?:^|_)((?:19|20)\d{2})$")
    year_column_pairs = []
    for key in first_row.keys():
        key_text = str(key)
        match = year_key_pattern.search(key_text.lower())
        if match:
            year_column_pairs.append((int(match.group(1)), key_text))

    if len({year for year, _ in year_column_pairs}) < 2:
        return rows

    year_totals = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        for year, column_name in year_column_pairs:
            numeric_value = _try_parse_float(row.get(column_name))
            if numeric_value is None:
                continue
            year_totals[year] = year_totals.get(year, 0.0) + numeric_value

    if len(year_totals) < 2:
        return rows

    reshaped_rows = []
    for year in sorted(year_totals.keys()):
        total_value = year_totals[year]
        if float(total_value).is_integer():
            total_value = int(total_value)
        else:
            total_value = round(total_value, 2)
        reshaped_rows.append({"delivery_year": year, "package_count": total_value})

    return reshaped_rows


def _try_parse_datetime(value):
    """Best-effort datetime parser for chart inference."""
    if value is None:
        return None
    text_value = str(value).strip()
    if not text_value or text_value.lower() in EMPTY_DATE_VALUES:
        return None

    for pattern in (
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m-%d-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(text_value[:19], pattern)
        except ValueError:
            continue
    return None


def _chart_point_and_category_limits(user_query):
    """Return chart limits; chart-full-data mode disables truncation."""
    if should_fetch_full_data_for_chart_query(user_query):
        return None, None
    return CHART_MAX_POINTS, CHART_MAX_CATEGORIES


def _force_requested_chart_type(chart_spec, requested_chart_type):
    """Force explicit chart type from user query onto inferred chart spec."""
    if not isinstance(chart_spec, dict) or not requested_chart_type:
        return chart_spec

    requested = str(requested_chart_type).strip().lower()
    if requested not in {"bar", "line", "pie", "scatter", "histogram"}:
        return chart_spec

    spec = dict(chart_spec)
    labels = list(spec.get("labels") or [])
    values = list(spec.get("values") or [])
    x_values = list(spec.get("x_values") or [])
    y_values = list(spec.get("y_values") or [])

    if requested in {"bar", "line", "pie"}:
        if labels and values:
            spec["chart_type"] = requested
            spec["point_count"] = len(values)
            return spec

        if x_values and y_values and len(x_values) == len(y_values):
            labels = [_format_numeric_label(value) for value in x_values]
            values = y_values
            spec["labels"] = labels
            spec["values"] = values
            spec["x_field"] = spec.get("x_field", "x")
            spec["y_field"] = spec.get("y_field", "value")
            spec["chart_type"] = requested
            spec["point_count"] = len(values)
            return spec

        if values:
            labels = [str(index + 1) for index in range(len(values))]
            spec["labels"] = labels
            spec["values"] = values
            spec["x_field"] = spec.get("x_field", "index")
            spec["y_field"] = spec.get("y_field", "value")
            spec["chart_type"] = requested
            spec["point_count"] = len(values)
            return spec

    if requested == "scatter":
        if x_values and y_values and len(x_values) == len(y_values):
            spec["chart_type"] = "scatter"
            spec["point_count"] = len(x_values)
            return spec

        if labels and values and len(labels) == len(values):
            numeric_x = [_try_parse_float(label) for label in labels]
            if all(value is not None for value in numeric_x):
                x_values = [float(value) for value in numeric_x]
            else:
                x_values = [float(index + 1) for index in range(len(values))]
            spec["x_values"] = x_values
            spec["y_values"] = values
            spec["x_field"] = spec.get("x_field", "x")
            spec["y_field"] = spec.get("y_field", "value")
            spec["chart_type"] = "scatter"
            spec["point_count"] = len(values)
            return spec

    if requested == "histogram":
        if values:
            spec["values"] = values
            spec["chart_type"] = "histogram"
            spec["point_count"] = len(values)
            return spec

        if y_values:
            spec["values"] = y_values
            spec["chart_type"] = "histogram"
            spec["point_count"] = len(y_values)
            return spec

    return spec


def _infer_column_kinds(rows):
    """Infer column kinds (numeric/date/category) from returned rows."""
    if not rows:
        return {}

    kinds = {}
    columns = [str(column) for column in rows[0].keys()]
    for column in columns:
        values = [row.get(column) for row in rows if isinstance(row, dict)]
        sample_values = [value for value in values if value not in (None, "")]
        if not sample_values:
            kinds[column] = "category"
            continue

        numeric_hits = sum(1 for value in sample_values if _try_parse_float(value) is not None)
        date_hits = sum(1 for value in sample_values if _try_parse_datetime(value) is not None)
        sample_count = len(sample_values)

        if numeric_hits >= max(2, int(sample_count * 0.7)):
            kinds[column] = "numeric"
        elif date_hits >= max(2, int(sample_count * 0.7)):
            kinds[column] = "date"
        else:
            kinds[column] = "category"
    return kinds


def _build_chart_spec_from_rows(rows, user_query):
    """Select a suitable chart spec deterministically from tabular rows."""
    if not isinstance(rows, list) or not rows:
        return None

    requested_chart_type = detect_requested_chart_type(user_query)

    def _finalize_chart_spec(spec):
        return _force_requested_chart_type(spec, requested_chart_type)

    ranking_or_count_query = is_ranking_or_count_query(user_query)
    trend_intent = _has_trend_intent(user_query)
    text = normalize_intent_text(user_query)
    kinds = _infer_column_kinds(rows)
    numeric_columns = [column for column, kind in kinds.items() if kind == "numeric"]
    date_columns = [column for column, kind in kinds.items() if kind == "date"]
    category_columns = [column for column, kind in kinds.items() if kind == "category"]

    max_points, max_categories = _chart_point_and_category_limits(user_query)
    chart_rows = rows if max_points is None else rows[:max_points]

    # Year-vs-count should be treated as a categorical comparison chart, even when
    # year values are parseable as numbers (e.g., 2025, 2026), to avoid scatter fallback.
    first_row_keys = {str(key).lower(): str(key) for key in rows[0].keys()} if isinstance(rows[0], dict) else {}
    year_key = first_row_keys.get("delivery_year") or first_row_keys.get("year")
    count_key = first_row_keys.get("package_count") or first_row_keys.get("count")
    if year_key and count_key:
        points = []
        for row in chart_rows:
            if not isinstance(row, dict):
                continue
            year_value = row.get(year_key)
            count_value = _try_parse_float(row.get(count_key))
            if year_value in (None, "") or count_value is None:
                continue
            year_label = str(year_value).strip()
            if not year_label:
                continue
            points.append((year_label, count_value))

        if len(points) >= 1:
            points.sort(key=lambda item: item[0])
            labels = [label for label, _ in points]
            values = [value for _, value in points]

            if requested_chart_type in ("line", "bar"):
                chart_type = requested_chart_type
            elif "year" in text:
                chart_type = "bar"
            else:
                chart_type = "bar"

            return _finalize_chart_spec({
                "chart_type": chart_type,
                "title": f"{count_key} by {year_key}",
                "x_field": year_key,
                "y_field": count_key,
                "labels": labels,
                "values": values,
                "point_count": len(values),
            })

    # Status-focused requests should aggregate counts by status and compare as bars.
    status_focused_query = any(term in text for term in ("active", "inactive", "status"))
    status_field = _find_status_field(rows)
    if status_focused_query and status_field:
        status_counts = {}
        for row in rows:
            label = _status_label_from_value(row.get(status_field))
            status_counts[label] = status_counts.get(label, 0) + 1

        # If user asks specifically for active/inactive comparison, always show both buckets.
        if "active" in text and "inactive" in text:
            status_counts.setdefault("Active", 0)
            status_counts.setdefault("Inactive", 0)

        if len(status_counts) >= 1:
            sorted_items = sorted(status_counts.items(), key=lambda item: item[1], reverse=True)
            trimmed_status_items = sorted_items if max_categories is None else sorted_items[:max_categories]
            labels = [item[0] for item in trimmed_status_items]
            values = [item[1] for item in trimmed_status_items]

            if requested_chart_type in ("pie", "bar"):
                chart_type = requested_chart_type
            else:
                chart_type = "pie" if _should_use_pie_by_intent(user_query, labels) else "bar"

            return _finalize_chart_spec({
                "chart_type": chart_type,
                "title": f"Recipient status counts by {status_field}",
                "x_field": status_field,
                "y_field": "count",
                "labels": labels,
                "values": values,
                "point_count": len(values),
            })

    if date_columns and numeric_columns:
        # Use time-series charts only for explicit line/bar requests or trend intent.
        if requested_chart_type in ("line", "bar") or trend_intent:
            x_field = date_columns[0]
            y_field = numeric_columns[0]
            points = []
            for row in chart_rows:
                parsed_date = _try_parse_datetime(row.get(x_field))
                parsed_value = _try_parse_float(row.get(y_field))
                if parsed_date is None or parsed_value is None:
                    continue
                points.append((parsed_date, parsed_value))

            if len(points) >= 2:
                points.sort(key=lambda item: item[0])
                labels = [item[0].strftime("%m/%d/%Y") for item in points]
                values = [item[1] for item in points]
                chart_type = "line"
                if requested_chart_type in ("line", "bar"):
                    chart_type = requested_chart_type

                return _finalize_chart_spec({
                    "chart_type": chart_type,
                    "title": "Trend over time",
                    "x_field": x_field,
                    "y_field": y_field,
                    "labels": labels,
                    "values": values,
                    "point_count": len(values),
                })

    if category_columns and numeric_columns:
        x_field = _choose_best_category_column(category_columns, user_query)
        percentage_column = next((column for column in numeric_columns if "percent" in str(column).lower()), "")
        count_column = next((column for column in numeric_columns if "count" in str(column).lower()), "")

        # Preserve row-level points for ranking/count charts when labels repeat
        # (for example multiple recipient_id values sharing the same recipient_name).
        # Otherwise, label aggregation can collapse N SQL rows into fewer chart points.
        valid_rows = []
        for row in chart_rows:
            if not isinstance(row, dict):
                continue
            category_key = str(row.get(x_field, "")).strip()
            if not category_key:
                continue
            valid_rows.append(row)

        if valid_rows and ranking_or_count_query:
            duplicate_counts = {}
            for row in valid_rows:
                key = str(row.get(x_field, "")).strip()
                duplicate_counts[key] = duplicate_counts.get(key, 0) + 1

            has_duplicate_labels = any(count > 1 for count in duplicate_counts.values())
            if has_duplicate_labels:
                first_row = valid_rows[0]
                id_field = ""
                for candidate in ("recipient_id", "package_id", "id", "tracking_number"):
                    if candidate in first_row:
                        id_field = candidate
                        break

                row_labels = []
                row_values = []
                label_occurrences = {}
                for row in valid_rows:
                    base_label = str(row.get(x_field, "")).strip()
                    metric_value = None
                    if count_column:
                        metric_value = _try_parse_float(row.get(count_column))
                    elif percentage_column:
                        metric_value = _try_parse_float(row.get(percentage_column))
                    else:
                        metric_value = _try_parse_float(row.get(numeric_columns[0]))

                    if metric_value is None:
                        continue

                    label = base_label
                    if duplicate_counts.get(base_label, 0) > 1:
                        id_value = row.get(id_field) if id_field else None
                        if id_value not in (None, ""):
                            label = f"{base_label} ({id_field}:{id_value})"

                    seen_count = label_occurrences.get(label, 0)
                    if seen_count > 0:
                        label = f"{label} #{seen_count + 1}"
                    label_occurrences[label] = seen_count + 1

                    row_labels.append(label)
                    row_values.append(metric_value)

                if len(row_values) >= 2:
                    chart_type = requested_chart_type if requested_chart_type in ("bar", "line") else "bar"
                    return _finalize_chart_spec(
                        {
                            "chart_type": chart_type,
                            "title": f"{count_column or percentage_column or numeric_columns[0]} by {x_field}",
                            "x_field": x_field,
                            "y_field": count_column or percentage_column or numeric_columns[0],
                            "labels": row_labels,
                            "values": row_values,
                            "point_count": len(row_values),
                        }
                    )

        category_map = {}
        for row in chart_rows:
            category_key = str(row.get(x_field, "")).strip()
            if not category_key:
                continue

            bucket = category_map.setdefault(category_key, {"count": 0.0, "percentage": 0.0})
            if count_column:
                parsed_count = _try_parse_float(row.get(count_column))
                if parsed_count is not None:
                    bucket["count"] += parsed_count
            if percentage_column:
                parsed_percentage = _try_parse_float(row.get(percentage_column))
                if parsed_percentage is not None:
                    bucket["percentage"] += parsed_percentage

            if not count_column and not percentage_column:
                default_metric = _try_parse_float(row.get(numeric_columns[0]))
                if default_metric is not None:
                    bucket["count"] += default_metric

        if len(category_map) >= 2:
            sort_key = "percentage" if percentage_column else "count"
            sorted_items = sorted(
                category_map.items(),
                key=lambda item: item[1].get(sort_key, 0.0),
                reverse=True,
            )
            trimmed_items = sorted_items if max_categories is None else sorted_items[:max_categories]
            labels = [item[0] for item in trimmed_items]
            count_values = [item[1].get("count", 0.0) for item in trimmed_items]
            percentage_values = [item[1].get("percentage", 0.0) for item in trimmed_items]

            # For percentage/distribution pie charts, use count as the geometry basis so pie labels
            # and absolute values stay meaningful, while also carrying percentages as metadata.
            if count_column:
                values = count_values
                y_field = count_column
            elif percentage_column:
                values = percentage_values
                y_field = percentage_column
            else:
                values = count_values
                y_field = numeric_columns[0]

            if requested_chart_type in ("pie", "bar"):
                chart_type = requested_chart_type
            else:
                # Dynamic policy:
                # - ranking/count asks default to bar for value comparison
                # - percentage/distribution asks can use pie when readable
                if ranking_or_count_query and not _should_use_pie_by_intent(user_query, labels):
                    chart_type = "bar"
                else:
                    chart_type = "pie" if _should_use_pie_by_intent(user_query, labels) else "bar"
            chart_spec = {
                "chart_type": chart_type,
                "title": f"{y_field} by {x_field}",
                "x_field": x_field,
                "y_field": y_field,
                "labels": labels,
                "values": values,
                "point_count": len(values),
            }
            if percentage_column:
                chart_spec["percentage_values"] = percentage_values
            if count_column:
                chart_spec["count_values"] = count_values
            return _finalize_chart_spec(chart_spec)

    if len(numeric_columns) >= 2:
        x_field = numeric_columns[0]
        y_field = numeric_columns[1]
        x_values = []
        y_values = []
        for row in chart_rows:
            x_value = _try_parse_float(row.get(x_field))
            y_value = _try_parse_float(row.get(y_field))
            if x_value is None or y_value is None:
                continue
            x_values.append(x_value)
            y_values.append(y_value)

        if len(x_values) >= 2:
            return _finalize_chart_spec({
                "chart_type": "scatter",
                "title": f"{y_field} vs {x_field}",
                "x_field": x_field,
                "y_field": y_field,
                "x_values": x_values,
                "y_values": y_values,
                "point_count": len(x_values),
            })

    if len(numeric_columns) == 1:
        x_field = numeric_columns[0]
        values = []
        for row in chart_rows:
            parsed = _try_parse_float(row.get(x_field))
            if parsed is not None:
                values.append(parsed)

        if len(values) >= 2:
            return _finalize_chart_spec({
                "chart_type": "histogram",
                "title": f"Distribution of {x_field}",
                "x_field": x_field,
                "values": values,
                "point_count": len(values),
            })

    return None


def _render_chart_as_base64_png(chart_spec):
    """Render inferred chart as base64 PNG using matplotlib."""
    if not MATPLOTLIB_AVAILABLE or not chart_spec:
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("CHART_RENDER | skipped | reason=matplotlib_unavailable | error=%s", MATPLOTLIB_IMPORT_ERROR)
        elif not chart_spec:
            logger.warning("CHART_RENDER | skipped | reason=empty_chart_spec")
        return ""

    figure = None
    try:
        figure, axis = plt.subplots(figsize=(10.4, 5.8), dpi=140)
        chart_type = chart_spec.get("chart_type")
        title = chart_spec.get("title", "Result chart")
        x_field = chart_spec.get("x_field", "x")
        y_field = chart_spec.get("y_field", "value")

        if chart_type == "line":
            labels = chart_spec["labels"]
            values = chart_spec["values"]
            axis.plot(labels, values, marker="o", linewidth=2.2, color="#1f77b4", label=y_field)
            axis.set_xlabel(x_field)
            axis.set_ylabel(y_field)
            axis.tick_params(axis="x", rotation=35, labelsize=8)
            axis.legend(loc="upper left", frameon=True)
            for index in _compute_annotation_indices(len(values)):
                value = values[index]
                axis.annotate(
                    _format_numeric_label(value),
                    (index, value),
                    textcoords="offset points",
                    xytext=(0, 6),
                    ha="center",
                    fontsize=7,
                    color="#163543",
                )
        elif chart_type == "bar":
            labels = chart_spec["labels"]
            values = chart_spec["values"]
            bars = axis.bar(labels, values, color="#ff7f11", edgecolor="#d66500", label=y_field)
            axis.set_xlabel(x_field)
            axis.set_ylabel(y_field)
            axis.tick_params(axis="x", rotation=30, labelsize=8)
            axis.legend(loc="upper right", frameon=True)
            if values:
                max_value = max(values)
                if max_value > 0:
                    axis.set_ylim(0, max_value * 1.18)
            label_indices = set(_compute_annotation_indices(len(values)))
            bar_labels = [
                _format_numeric_label(value) if index in label_indices else ""
                for index, value in enumerate(values)
            ]
            axis.bar_label(bars, labels=bar_labels, padding=4, fontsize=9, color="#102027", fontweight="bold")
        elif chart_type == "pie":
            labels = chart_spec["labels"]
            values = chart_spec["values"]
            total = sum(values) if values else 0
            count_values = chart_spec.get("count_values") or []
            percentage_values = chart_spec.get("percentage_values") or []

            def _pie_value_formatter(pct):
                absolute = (pct * total / 100.0) if total else 0.0
                return f"{pct:.1f}%\n({_format_numeric_label(absolute)})"

            wedges, _, _ = axis.pie(
                values,
                labels=None,
                autopct=_pie_value_formatter,
                startangle=120,
                pctdistance=0.78,
                textprops={"fontsize": 8},
            )
            axis.axis("equal")
            legend_labels = []
            for index, label in enumerate(labels):
                count_value = count_values[index] if index < len(count_values) else (values[index] if index < len(values) else 0)
                percentage_value = percentage_values[index] if index < len(percentage_values) else None
                if percentage_value is None:
                    legend_labels.append(f"{label}: {_format_numeric_label(count_value)}")
                else:
                    legend_labels.append(
                        f"{label}: {_format_numeric_label(count_value)} ({_format_numeric_label(percentage_value)}%)"
                    )
            axis.legend(
                wedges,
                legend_labels,
                title=x_field,
                loc="center left",
                bbox_to_anchor=(1.02, 0.5),
                frameon=True,
                fontsize=8,
                title_fontsize=9,
            )
        elif chart_type == "scatter":
            x_values = chart_spec["x_values"]
            y_values = chart_spec["y_values"]
            axis.scatter(x_values, y_values, color="#2a9d8f", alpha=0.85, label=y_field)
            axis.set_xlabel(x_field)
            axis.set_ylabel(y_field)
            axis.legend(loc="upper left", frameon=True)
            # Annotate scatter points dynamically based on dataset size.
            for index in _compute_annotation_indices(len(x_values)):
                x_value = x_values[index]
                y_value = y_values[index]
                label_text = f"({_format_numeric_label(x_value)}, {_format_numeric_label(y_value)})"
                axis.annotate(
                    label_text,
                    (x_value, y_value),
                    textcoords="offset points",
                    xytext=(5, 5),
                    ha="left",
                    fontsize=7,
                    color="#163543",
                )
        elif chart_type == "histogram":
            values = chart_spec["values"]
            bin_count = min(12, max(4, len(values) // 2))
            counts, bins, _ = axis.hist(values, bins=bin_count, color="#264653", label=x_field)
            axis.set_xlabel(x_field)
            axis.set_ylabel("Frequency")
            axis.legend(loc="upper right", frameon=True)
            for index in _compute_annotation_indices(len(counts)):
                count_value = counts[index]
                if count_value <= 0:
                    continue
                bin_center = (bins[index] + bins[index + 1]) / 2.0
                axis.annotate(
                    _format_numeric_label(count_value),
                    (bin_center, count_value),
                    textcoords="offset points",
                    xytext=(0, 6),
                    ha="center",
                    fontsize=7,
                    color="#163543",
                )
        else:
            logger.warning("CHART_RENDER | skipped | reason=unsupported_chart_type | chart_type=%s", chart_type)
            plt.close(figure)
            return ""

        axis.set_title(title)
        axis.grid(alpha=0.2, linewidth=0.6)
        figure.tight_layout()

        buffer = io.BytesIO()
        figure.savefig(buffer, format="png")
        buffer.seek(0)
        encoded = base64.b64encode(buffer.read()).decode("ascii")
        logger.info(
            "CHART_RENDER | success | chart_type=%s | points=%s",
            chart_type,
            chart_spec.get("point_count", 0),
        )
        return encoded
    except Exception as error:
        logger.exception("CHART_RENDER | failed | error=%s", error)
        return ""
    finally:
        if figure is not None:
            plt.close(figure)


def build_chart_payload(rows, user_query):
    """Build chart payload from rows when chart mode is requested."""
    if not isinstance(rows, list) or not rows:
        logger.warning("CHART_PAYLOAD | skipped | reason=no_rows")
        return None

    chart_spec = _build_chart_spec_from_rows(_mask_rows_for_display(rows), user_query)
    if not chart_spec:
        logger.warning("CHART_PAYLOAD | skipped | reason=no_chart_spec")
        return None

    chart_image_base64 = ""
    if SERVER_SIDE_CHART_IMAGE_RENDER:
        chart_image_base64 = _render_chart_as_base64_png(chart_spec)
        if not chart_image_base64:
            logger.warning(
                "CHART_PAYLOAD | continuing_without_image | chart_type=%s",
                chart_spec.get("chart_type"),
            )

    payload = {
        "chart_type": chart_spec.get("chart_type"),
        "title": chart_spec.get("title"),
        "x_field": chart_spec.get("x_field", ""),
        "y_field": chart_spec.get("y_field", ""),
        "point_count": chart_spec.get("point_count", 0),
        "image_base64": chart_image_base64,
        "labels": chart_spec.get("labels", []),
        "values": chart_spec.get("values", []),
        "count_values": chart_spec.get("count_values", []),
        "percentage_values": chart_spec.get("percentage_values", []),
        "x_values": chart_spec.get("x_values", []),
        "y_values": chart_spec.get("y_values", []),
    }
    logger.info(
        "CHART_PAYLOAD | success | chart_type=%s | points=%s",
        payload.get("chart_type"),
        payload.get("point_count", 0),
    )
    return payload


def update_chart_context(display_mode, chart_payload):
    """Persist chart payload for follow-up questions in the same session."""
    if (
        display_mode == "chart"
        and isinstance(chart_payload, dict)
        and chart_payload
        and not chart_payload.get("render_error")
    ):
        session["last_chart_payload"] = chart_payload
    else:
        session.pop("last_chart_payload", None)


def attach_dashboard_filter_context(chart_payload, filter_kind, user_query, row_limit=None):
    """Attach backend filter context so dashboard can recalculate charts by date range."""
    if not isinstance(chart_payload, dict) or not chart_payload:
        return chart_payload

    kind = str(filter_kind or "").strip().lower()
    if not kind:
        return chart_payload

    context = {
        "kind": kind,
        "user_query": str(user_query or "").strip(),
    }
    if row_limit is not None:
        try:
            context["row_limit"] = max(1, min(int(row_limit), 200))
        except (TypeError, ValueError):
            pass

    chart_payload["dashboard_filter_context"] = context
    return chart_payload


def resolve_visual_response(user_query, rows, default_display, chart_source_rows=None):
    """Resolve final display mode among text/table/key_value/chart."""
    chart_rows = chart_source_rows if isinstance(chart_source_rows, list) and chart_source_rows else rows
    operation_profile = resolve_requested_operation(user_query)

    if operation_profile.get("mode") == "text":
        logger.info(
            "VISUAL_MODE | requested_chart=false | resolved=text | rows=%s",
            len(rows) if isinstance(rows, list) else 0,
        )
        return "text", [], None

    explicit_chart_request = bool(operation_profile.get("mode") == "chart" or operation_profile.get("explicit_chart"))
    specific_chart_request = is_specific_chart_request(user_query)
    auto_chart_request = should_auto_chart_view(user_query, chart_rows)

    if explicit_chart_request or auto_chart_request:
        chart_payload = build_chart_payload(chart_rows, user_query)
        if chart_payload:
            logger.info(
                "VISUAL_MODE | requested_chart=%s | auto_chart=%s | resolved=chart | chart_type=%s | points=%s | rows=%s",
                explicit_chart_request,
                auto_chart_request,
                chart_payload.get("chart_type"),
                chart_payload.get("point_count", 0),
                len(chart_rows) if isinstance(chart_rows, list) else 0,
            )
            return "chart", rows, chart_payload
        # If chart is requested (explicit or automatic) but cannot be rendered,
        # keep tabular output instead of key_value fallback.
        if explicit_chart_request and specific_chart_request:
            logger.warning(
                "VISUAL_MODE | requested_chart=true | specific_chart=true | resolved=chart_unavailable | chart_payload_missing=true | rows=%s",
                len(rows) if isinstance(rows, list) else 0,
            )
            return "chart", [], build_chart_unavailable_payload(user_query)

        logger.warning(
            "VISUAL_MODE | requested_chart=%s | auto_chart=%s | resolved=table_fallback | chart_payload_missing=true | rows=%s",
            explicit_chart_request,
            auto_chart_request,
            len(rows) if isinstance(rows, list) else 0,
        )
        return "table", rows, None

    if default_display == "text":
        logger.info(
            "VISUAL_MODE | requested_chart=false | resolved=text | reason=default_text | rows=%s",
            len(rows) if isinstance(rows, list) else 0,
        )
        return "text", [], None
    logger.info(
        "VISUAL_MODE | requested_chart=false | resolved=%s | rows=%s",
        default_display,
        len(rows) if isinstance(rows, list) else 0,
    )
    return default_display, rows, None


def is_report_request(user_query):
    """Detect user commands requesting report generation."""
    text = normalize_intent_text(user_query)
    if not text:
        return False
    return any(re.search(pattern, text) for pattern in REPORT_MODE_PATTERNS)


def _build_report_title(user_query):
    """Create report title from user query."""
    cleaned = re.sub(r"\s+", " ", str(user_query or "").strip())
    if not cleaned:
        return "Account Data Report"
    if len(cleaned) > 80:
        cleaned = cleaned[:77].rstrip() + "..."
    return f"Report: {cleaned}"


def _build_report_kpis(rows):
    """Compute lightweight KPI cards for report view."""
    if not isinstance(rows, list):
        rows = []

    kpis = [
        {"label": "Records", "value": len(rows)},
    ]

    if not rows or not isinstance(rows[0], dict):
        return kpis

    columns = [str(column) for column in rows[0].keys()]
    kpis.append({"label": "Columns", "value": len(columns)})

    kinds = _infer_column_kinds(rows)
    numeric_columns = [column for column, kind in kinds.items() if kind == "numeric"]
    date_columns = [column for column, kind in kinds.items() if kind == "date"]

    if numeric_columns:
        metric_col = numeric_columns[0]
        values = []
        for row in rows:
            value = _try_parse_float(row.get(metric_col))
            if value is not None:
                values.append(value)
        if values:
            kpis.append({"label": f"Sum({metric_col})", "value": round(sum(values), 2)})
            kpis.append({"label": f"Avg({metric_col})", "value": round(sum(values) / len(values), 2)})

    if date_columns:
        date_col = date_columns[0]
        parsed_dates = []
        for row in rows:
            parsed = _try_parse_datetime(row.get(date_col))
            if parsed is not None:
                parsed_dates.append(parsed)
        if parsed_dates:
            kpis.append(
                {
                    "label": f"Date Range ({date_col})",
                    "value": f"{min(parsed_dates).strftime('%m/%d/%Y')} to {max(parsed_dates).strftime('%m/%d/%Y')}",
                }
            )

    return kpis


def resolve_report_view_response(user_query, table_rows, chart_source_rows=None):
    """Resolve report display mode and payload without file downloads."""
    chart_rows = chart_source_rows if isinstance(chart_source_rows, list) and chart_source_rows else table_rows

    if query_requests_text_only_view(user_query):
        return "text", [], None

    if query_requests_chart_view(user_query) or should_auto_chart_view(user_query, chart_rows):
        chart_payload = build_chart_payload(chart_rows, user_query)
        if chart_payload:
            return "chart", table_rows, chart_payload
        if is_specific_chart_request(user_query):
            return "chart", [], build_chart_unavailable_payload(user_query)

    if query_requests_table_view(user_query):
        return "table", table_rows, None

    return "table", table_rows, None


def build_report_payload(user_query, generated_sql, table_rows, display_mode, chart_payload):
    """Build on-screen report payload for frontend rendering."""
    return {
        "title": _build_report_title(user_query),
        "generated_at": datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
        "display_mode": display_mode,
        "kpis": _build_report_kpis(table_rows),
        "sql": normalize_generated_sql_for_log(generated_sql),
        "has_chart": bool(chart_payload),
    }


def is_chart_followup_question(user_query):
    """Return True for likely follow-up questions about previously generated chart."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    def _has_term(term):
        # Single-word terms should match on word boundaries only,
        # so 'count' does not match inside 'account'.
        if " " in term:
            return term in text
        return re.search(rf"\b{re.escape(term)}\b", text) is not None

    has_chart_ref = any(re.search(pattern, text) for pattern in CHART_FOLLOWUP_PATTERNS)
    asks_metric = any(
        _has_term(token)
        for token in (
            "highest",
            "lowest",
            "max",
            "min",
            "total",
            "sum",
            "average",
            "avg",
            "mean",
            "count",
            "point",
            "points",
            "trend",
            "increase",
            "decrease",
            "x axis",
            "y axis",
            "type",
            "title",
            "what",
            "which",
            "how many",
        )
    )

    # Guardrail: full data requests should not be hijacked by chart follow-up mode.
    full_data_request_terms = (
        "generate",
        "visualize",
        "visualise",
        "compare",
        "comparison",
        "show",
        "list",
        "top",
        "recipient",
        "recipients",
        "package",
        "packages",
        "delivered",
        "report",
        "table",
    )
    if any(term in text for term in full_data_request_terms):
        return False

    if has_chart_ref and asks_metric:
        return True

    # Allow short metric-only follow-ups right after chart generation,
    # for example "highest?", "total", "average value".
    token_count = len(text.split())
    return asks_metric and token_count <= 5


def _chart_label_value_pairs(chart_payload):
    """Get safe label/value pairs for chart metrics."""
    labels = chart_payload.get("labels") or []
    values = chart_payload.get("values") or []
    pairs = []
    for index, label in enumerate(labels):
        if index >= len(values):
            break
        value = _try_parse_float(values[index])
        if value is None:
            continue
        pairs.append((str(label), value))
    return pairs


def answer_from_last_chart(user_query, chart_payload):
    """Build deterministic answer for user follow-up questions on previously generated chart."""
    if not isinstance(chart_payload, dict) or not chart_payload:
        return ""

    text = normalize_intent_text(user_query)
    chart_type = str(chart_payload.get("chart_type", "")).strip().lower()
    title = str(chart_payload.get("title", "")).strip() or "Generated chart"
    x_field = str(chart_payload.get("x_field", "")).strip() or "x"
    y_field = str(chart_payload.get("y_field", "")).strip() or "value"
    point_count = int(chart_payload.get("point_count", 0) or 0)
    pairs = _chart_label_value_pairs(chart_payload)

    if "type" in text:
        return f"The chart type is {chart_type or 'chart'} with title '{title}'."

    if "title" in text or "about" in text:
        return f"The chart title is '{title}'."

    if "x axis" in text and "y axis" in text:
        return f"The x-axis is {x_field} and the y-axis is {y_field}."
    if "x axis" in text:
        return f"The x-axis field is {x_field}."
    if "y axis" in text:
        return f"The y-axis field is {y_field}."

    if "point" in text or "points" in text or "how many" in text:
        if point_count > 0:
            return f"The chart contains {point_count} plotted data point(s)."

    if pairs:
        highest = max(pairs, key=lambda item: item[1])
        lowest = min(pairs, key=lambda item: item[1])
        total = sum(value for _, value in pairs)
        average = total / len(pairs)

        if any(token in text for token in ("highest", "max", "top", "most")):
            return f"The highest value is {highest[1]:.2f} for {highest[0]}."

        if any(token in text for token in ("lowest", "min", "least")):
            return f"The lowest value is {lowest[1]:.2f} for {lowest[0]}."

        if any(token in text for token in ("total", "sum")):
            return f"The total across the chart values is {total:.2f}."

        if any(token in text for token in ("average", "avg", "mean")):
            return f"The average chart value is {average:.2f}."

        if any(token in text for token in ("trend", "increase", "decrease")) and chart_type == "line":
            first_label, first_value = pairs[0]
            last_label, last_value = pairs[-1]
            if last_value > first_value:
                direction = "increasing"
            elif last_value < first_value:
                direction = "decreasing"
            else:
                direction = "flat"
            return (
                f"The overall line trend is {direction}: {first_value:.2f} at {first_label} "
                f"to {last_value:.2f} at {last_label}."
            )

    return "I can answer chart follow-up questions such as highest, lowest, total, average, trend, chart type, and axis fields."


def is_display_mode_followup_request(user_query):
    """Detect explicit request to re-render already fetched rows in table/chart mode."""
    text = normalize_intent_text(user_query)
    if any(re.search(pattern, text) for pattern in DISPLAY_MODE_PATTERNS):
        return True

    # Support follow-up asks like "show this in bar chart" or "visualize this".
    operation_profile = resolve_requested_operation(text)
    if operation_profile.get("mode") == "chart":
        return True

    return False


def is_format_only_followup_request(user_query):
    """Detect pure format-switch requests without fresh data intent."""
    text = normalize_intent_text(user_query)
    if not is_display_mode_followup_request(text):
        return False

    has_content_intent = any(term in text for term in CONTENT_QUERY_TERMS)
    if has_content_intent:
        return False

    # Treat as follow-up only when user clearly refers to prior context,
    # such as "show this in table format".
    context_reference_terms = (
        "this",
        "that",
        "these",
        "those",
        "same",
        "above",
        "previous",
        "earlier",
        "them",
        "it",
    )
    return any(re.search(rf"\b{re.escape(term)}\b", text) for term in context_reference_terms)


def extract_requested_row_limit(user_query, max_limit=100):
    """Extract user-requested row count (for example 'last 2 records')."""
    text = normalize_intent_text(user_query)

    for pattern in ROW_LIMIT_PATTERNS:
        match = re.search(pattern, text)
        if match:
            requested = int(match.group(1))
            return max(1, min(requested, int(max_limit)))

    # Fallback parser for typo-heavy phrasing like "latest 15 recoerds".
    tokens = re.findall(r"[a-z0-9']+", text)
    if not tokens:
        return None

    quantity_units = {
        "record",
        "records",
        "row",
        "rows",
        "package",
        "packages",
        "item",
        "items",
        "result",
        "results",
    }
    quantity_context = {
        "last",
        "latest",
        "recent",
        "top",
        "first",
        "show",
        "list",
        "give",
        "fetch",
    }
    time_window_units = {"day", "days", "week", "weeks", "month", "months", "year", "years"}

    for index, token in enumerate(tokens):
        if not token.isdigit() or len(token) > 3:
            continue

        requested = int(token)
        if requested <= 0:
            continue

        next_token = tokens[index + 1] if index + 1 < len(tokens) else ""
        # Do not treat "last 5 months" style quantities as row limits.
        if next_token in time_window_units:
            continue

        window_start = max(0, index - 3)
        window_end = min(len(tokens), index + 4)
        window_tokens = tokens[window_start:window_end]

        has_context_term = any(candidate in quantity_context for candidate in window_tokens)
        has_quantity_unit = any(
            SequenceMatcher(None, candidate, unit).ratio() >= 0.72
            for candidate in window_tokens
            for unit in quantity_units
            if candidate.isalpha()
        )

        if has_context_term or has_quantity_unit:
            return max(1, min(requested, int(max_limit)))

    return None


def extract_ranked_limit_from_raw_query(user_query, max_limit=100):
    """Extract explicit rank limit from raw query text (top/bottom/first N)."""
    raw_text = str(user_query or "").strip().lower()
    if not raw_text:
        return None

    match = re.search(r"\b(?:top|bottom|first)\s+(\d{1,3})\b", raw_text)
    if not match:
        return None

    requested = int(match.group(1))
    return max(1, min(requested, int(max_limit)))


def should_enforce_top_n_limit(user_query):
    """Return True when query asks for ranked Top/Bottom N style output."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    # Fast path using raw text so normalization quirks do not hide top-N asks.
    if extract_ranked_limit_from_raw_query(user_query, max_limit=100) is not None:
        return True

    requested_limit = extract_requested_row_limit(text, max_limit=100)
    if not requested_limit:
        return False

    ranking_terms = (
        "top",
        "highest",
        "maximum",
        "max",
        "lowest",
        "least",
        "bottom",
        "first",
    )
    return any(term in text for term in ranking_terms)


def extract_requested_relative_time_window(user_query):
    """Extract relative date windows like 'last 5 months' for SQL filtering."""
    text = normalize_intent_text(user_query)
    if not text:
        return None

    pattern = re.search(
        r"\b(?:last|past)\s+(\d{1,3})\s*(day|days|week|weeks|month|months|year|years)\b",
        text,
    )
    if pattern:
        value = max(1, min(int(pattern.group(1)), 120))
        unit_text = pattern.group(2).lower()
        unit_map = {
            "day": "DAY",
            "days": "DAY",
            "week": "WEEK",
            "weeks": "WEEK",
            "month": "MONTH",
            "months": "MONTH",
            "year": "YEAR",
            "years": "YEAR",
        }
        return {
            "value": value,
            "unit": unit_map.get(unit_text, "DAY"),
            "phrase": f"last {value} {unit_text}",
        }

    single_window = re.search(r"\blast\s+(day|week|month|year)\b", text)
    if single_window:
        unit_text = single_window.group(1).lower()
        unit_map = {
            "day": "DAY",
            "week": "WEEK",
            "month": "MONTH",
            "year": "YEAR",
        }
        return {
            "value": 1,
            "unit": unit_map.get(unit_text, "DAY"),
            "phrase": f"last {unit_text}",
        }

    return None


def build_relative_time_window_condition(user_query, date_column="date_received"):
    """Build SQL condition for relative windows like last 5 months/years."""
    window = extract_requested_relative_time_window(user_query)
    if not window:
        return None

    column = str(date_column or "date_received").strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", column):
        column = "date_received"

    window_value = max(1, min(int(window.get("value", 1)), 120))
    window_unit = str(window.get("unit", "DAY")).upper()
    if window_unit not in {"DAY", "WEEK", "MONTH", "YEAR"}:
        window_unit = "DAY"
    return f"{column} >= DATE_SUB(CURDATE(), INTERVAL {window_value} {window_unit})"


def _normalize_iso_date(value):
    """Normalize YYYY-MM-DD date string; return empty string when invalid."""
    text = str(value or "").strip()
    if not text:
        return ""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return ""
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return ""
    return text


def build_explicit_date_range_condition(date_column, start_date=None, end_date=None):
    """Build inclusive SQL DATE range condition for a date/datetime column."""
    column = str(date_column or "date_received").strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_\.]*", column):
        column = "date_received"

    start_iso = _normalize_iso_date(start_date)
    end_iso = _normalize_iso_date(end_date)
    if not start_iso and not end_iso:
        return ""

    if start_iso and end_iso and start_iso > end_iso:
        start_iso, end_iso = end_iso, start_iso

    conditions = []
    if start_iso:
        conditions.append(f"DATE({column}) >= {sql_literal(start_iso)}")
    if end_iso:
        conditions.append(f"DATE({column}) <= {sql_literal(end_iso)}")
    return " AND ".join(conditions)


def extract_requested_calendar_year(user_query):
    """Extract explicit calendar-year intent from natural-language query."""
    text = normalize_intent_text(user_query)
    if not text:
        return None

    if re.search(r"\bthis\s+year\b|\bcurrent\s+year\b", text):
        return int(date.today().year)

    if re.search(r"\blast\s+year\b|\bprevious\s+year\b", text):
        return int(date.today().year) - 1

    year_match = re.search(r"\b(?:19|20)\d{2}\b", text)
    if not year_match:
        return None

    year_value = int(year_match.group(0))
    if 1900 <= year_value <= 2100:
        return year_value
    return None


def extract_requested_calendar_years(user_query, max_years=6):
    """Extract distinct explicit calendar years from query text."""
    text = normalize_intent_text(user_query)
    if not text:
        return []

    matches = re.findall(r"\b(?:19|20)\d{2}\b", text)
    years = []
    for match in matches:
        year_value = int(match)
        if 1900 <= year_value <= 2100 and year_value not in years:
            years.append(year_value)
        if len(years) >= max(1, int(max_years)):
            break
    return sorted(years)


def extract_requested_year_bucket_list(user_query, max_years=6):
    """Extract a dynamic set of year buckets from explicit and relative year intent."""
    text = normalize_intent_text(user_query)
    if not text:
        return []

    max_items = max(1, int(max_years))
    year_set = set(extract_requested_calendar_years(text, max_years=max_items))
    current_year = int(date.today().year)

    if re.search(r"\b(?:this|current)(?:\s+financial)?\s+year\b", text):
        year_set.add(current_year)

    if re.search(r"\b(?:last|previous)(?:\s+financial)?\s+year\b", text):
        year_set.add(current_year - 1)

    rolling_years_match = re.search(r"\b(?:last|past)\s+(\d{1,2})\s+years?\b", text)
    if rolling_years_match:
        span = max(1, min(int(rolling_years_match.group(1)), max_items))
        for offset in range(span):
            year_set.add(current_year - offset)

    bounded_years = [year for year in sorted(year_set) if 1900 <= int(year) <= 2100]
    return bounded_years[:max_items]


def is_yearly_package_count_request(user_query):
    """Return True for prompts asking package counts by explicit year(s)."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    years = extract_requested_year_bucket_list(text)
    if not years:
        return False

    has_package_context = any(token in text for token in TOP_RECIPIENT_PACKAGE_TERMS)
    has_count = _contains_any_term(text, RECIPIENT_WISE_COUNT_TERMS)
    has_year_word = "year" in text or "years" in text
    has_delivered_context = "delivered" in text or "received" in text
    return has_package_context and (has_count or has_year_word or has_delivered_context)


def is_monthly_delivered_count_request(user_query):
    """Return True for prompts asking delivered package counts grouped by month."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    has_package_context = any(token in text for token in TOP_RECIPIENT_PACKAGE_TERMS)
    has_delivered_context = "delivered" in text or "received" in text
    relative_window = extract_requested_relative_time_window(text)
    has_month_window = bool(relative_window and str(relative_window.get("unit", "")).upper() == "MONTH")
    has_month_language = any(term in text for term in ("month", "months", "monthly"))
    has_count_or_chart = _has_count_intent(text) or query_requests_chart_view(text) or any(
        term in text for term in ("trend", "report", "comparison", "data")
    )

    # Keep dedicated carrier analytics intents in their own branches.
    if is_top_carrier_request(text) or is_carrier_wise_count_request(text) or is_carrier_percentage_request(text):
        return False

    return has_package_context and has_delivered_context and (has_month_window or has_month_language) and has_count_or_chart


def build_monthly_delivered_count_sql(
    account_id,
    user_query,
    start_date=None,
    end_date=None,
    override_time_filters=False,
):
    """Build deterministic month-vs-delivered-count SQL in account scope."""
    account_id_lit = sql_literal(account_id)
    conditions = [f"account_id = {account_id_lit}", "date_received IS NOT NULL"]

    explicit_date_condition = build_explicit_date_range_condition("date_received", start_date, end_date)
    if override_time_filters and explicit_date_condition:
        conditions.append(explicit_date_condition)
    else:
        if explicit_date_condition:
            conditions.append(explicit_date_condition)

        relative_condition = build_relative_time_window_condition(user_query, "date_received")
        if relative_condition:
            conditions.append(relative_condition)
        else:
            # Default to a practical trend window when month-wise aggregation is requested without explicit span.
            conditions.append("date_received >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)")

    where_sql = " AND ".join(conditions)
    return (
        "SELECT DATE_FORMAT(date_received, '%Y-%m') AS delivery_month, COUNT(*) AS package_count "
        "FROM track_packages "
        f"WHERE {where_sql} "
        "GROUP BY DATE_FORMAT(date_received, '%Y-%m') "
        "ORDER BY delivery_month"
    )


def build_yearly_package_count_sql(
    account_id,
    user_query,
    start_date=None,
    end_date=None,
    override_time_filters=False,
):
    """Build deterministic year-vs-package-count SQL for explicit year comparisons."""
    years = extract_requested_year_bucket_list(user_query)
    account_id_lit = sql_literal(account_id)

    conditions = [f"account_id = {account_id_lit}", "date_received IS NOT NULL"]
    explicit_date_condition = build_explicit_date_range_condition("date_received", start_date, end_date)
    if override_time_filters and explicit_date_condition:
        conditions.append(explicit_date_condition)
    else:
        if years:
            years_sql = ", ".join(str(year) for year in years)
            conditions.append(f"YEAR(date_received) IN ({years_sql})")
        if explicit_date_condition:
            conditions.append(explicit_date_condition)

    where_sql = " AND ".join(conditions)
    return (
        "SELECT YEAR(date_received) AS delivery_year, COUNT(*) AS package_count "
        "FROM track_packages "
        f"WHERE {where_sql} "
        "GROUP BY YEAR(date_received) "
        "ORDER BY delivery_year"
    )


def _strip_trailing_limit_clause(sql_text):
    """Remove trailing LIMIT/OFFSET to allow full dataset execution when needed."""
    normalized = normalize_generated_sql_for_log(sql_text)
    if not normalized:
        return ""

    # Handles: LIMIT 100, LIMIT 0, 100, LIMIT 100 OFFSET 20
    stripped = re.sub(
        r"\s+LIMIT\s+(?:\d+\s*,\s*\d+|\d+)(?:\s+OFFSET\s+\d+)?\s*$",
        "",
        normalized,
        flags=re.IGNORECASE,
    ).strip()
    return stripped or normalized


def should_preserve_generated_limit_for_chart(user_query):
    """Keep intent-driven LIMITs (top/max/min) when chart full-data mode is enabled."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    if extract_ranked_limit_from_raw_query(user_query, max_limit=100) is not None:
        return True
    if should_enforce_top_n_limit(user_query):
        return True

    peak_terms = ("maximum", "highest", "minimum", "lowest", "least", "max", "min")
    return any(term in text for term in peak_terms)


def query_mentions_package_id(user_query):
    """Return True when query explicitly asks for package-id style output."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    return bool(
        re.search(r"\bpackage\s*id(?:s)?\b", text)
        or re.search(r"\bpkg\s*id(?:s)?\b", text)
        or re.search(r"\bpackageid(?:s)?\b", text)
    )


def is_latest_packages_request(user_query):
    """Detect requests asking for latest/last packages dataset."""
    text = normalize_intent_text(user_query)
    has_package_term = any(token in text for token in ("package", "packages"))
    has_latest_term = any(token in text for token in LATEST_DATE_TERMS)
    has_date_context = any(token in text for token in ("latest", "last", "recent", "newest"))
    has_package_id_request = query_mentions_package_id(text)
    requested_limit = extract_requested_row_limit(text, max_limit=100)
    has_top_limit_style = "top" in text and requested_limit is not None

    # Treat "top N package ids" as a package-list intent, not recipient ranking.
    if has_top_limit_style and has_package_id_request:
        return True

    return has_package_term and (has_latest_term or has_date_context)


def detect_recipient_template_intent(user_query):
    """Detect deterministic core_recipients/core join use-cases."""
    text = normalize_intent_text(user_query)
    if not text:
        return "none"

    explicit_table_matches = _find_exact_table_name_matches(user_query, SUPPORTED_QUERY_TABLES)
    recipient_tables = {"core_recipients", "track_packages"}
    if any(table_name not in recipient_tables for table_name in explicit_table_matches):
        return "none"

    requested_status_keys = detect_requested_recipient_status_keys(text)
    has_member_word = re.search(r"\bmember\b|\bmembers\b", text) is not None
    has_recipient = _contains_any_term(text, RECIPIENT_TEMPLATE_KEYWORDS.get("recipient", []))
    has_package = _contains_any_term(text, RECIPIENT_TEMPLATE_KEYWORDS.get("package", []))
    has_contact = _contains_any_term(text, RECIPIENT_TEMPLATE_KEYWORDS.get("contact", []))
    asks_count = _contains_any_term(text, RECIPIENT_TEMPLATE_KEYWORDS.get("count", []))
    asks_listing = _contains_any_term(text, RECIPIENT_TEMPLATE_KEYWORDS.get("listing", []))
    asks_percentage = any(term in text for term in ("percentage", "percent", "share", "ratio", "distribution", "breakdown"))

    # Status-only prompts like "how many are inactive in this account"
    # should still route to core_recipients even if "recipient" is omitted.
    has_status_context = bool(requested_status_keys)

    if has_status_context and not has_package and (has_recipient or has_member_word or asks_count or asks_listing):
        # Percentage/share questions need row-level status values for visualization.
        if asks_count and not asks_percentage:
            return "recipient_count"
        return "recipient_directory"

    if has_recipient and asks_count and not has_package:
        return "recipient_count"
    if has_recipient and has_package:
        return "recipient_packages_join"
    if has_recipient and (has_contact or asks_listing or bool(requested_status_keys)):
        return "recipient_directory"
    return "none"


def should_use_recipient_accuracy_path(user_query):
    """Return True when recipient-related prompts should use deterministic accurate SQL path."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    if is_top_recipient_request(text) or is_recipient_wise_count_request(text):
        return True

    return detect_recipient_template_intent(text) != "none"


def detect_requested_recipient_status_keys(user_query):
    """Infer recipient status filters from natural language."""
    text = normalize_intent_text(user_query)
    if not text:
        return []

    def status_term_matches(message_text, term_text):
        """Match configured status term as phrase or whole word."""
        normalized_term = str(term_text).strip().lower()
        if not normalized_term:
            return False
        if " " in normalized_term:
            return normalized_term in message_text
        return re.search(rf"\b{re.escape(normalized_term)}\b", message_text) is not None

    status_keys = []
    prioritized_keys = []
    seen_status = set()
    for status_key in RECIPIENT_STATUS_PRIORITY:
        if status_key in RECIPIENT_STATUS_TERMS and status_key not in seen_status:
            prioritized_keys.append(status_key)
            seen_status.add(status_key)
    for status_key in RECIPIENT_STATUS_TERMS.keys():
        normalized_key = str(status_key).strip().lower()
        if normalized_key not in seen_status:
            prioritized_keys.append(normalized_key)
            seen_status.add(normalized_key)

    for status_key in prioritized_keys:
        terms = RECIPIENT_STATUS_TERMS.get(status_key, [])
        for term in terms:
            if status_term_matches(text, term):
                status_keys.append(str(status_key).strip().lower())
                break

    # Preserve order while removing duplicates.
    seen = set()
    ordered = []
    for item in status_keys:
        if item not in seen:
            seen.add(item)
            ordered.append(item)
    return ordered


def _build_recipient_status_sql_condition(column_name, status_keys):
    """Build SQL condition for recipient_status using configured codes and text aliases."""
    if not isinstance(status_keys, list) or not status_keys:
        return ""

    numeric_values = []
    text_values = []
    status_aliases = []

    for key in status_keys:
        normalized_key = str(key).strip().lower()
        if not normalized_key:
            continue

        # Include configured aliases as text fallbacks so status filters stay robust
        # when recipient_status storage shifts between numeric and text values.
        status_aliases.append(normalized_key)
        for alias in RECIPIENT_STATUS_TERMS.get(normalized_key, []):
            alias_text = str(alias).strip().lower()
            if alias_text:
                status_aliases.append(alias_text)

        for code in RECIPIENT_STATUS_CODE_MAP.get(normalized_key, []):
            code_text = str(code).strip().lower()
            if not code_text:
                continue
            if isinstance(code, bool) or isinstance(code, int) or isinstance(code, float):
                numeric_values.append(sql_literal(code))
                continue
            if re.fullmatch(r"\d+", code_text):
                numeric_values.append(sql_literal(int(code_text)))
            else:
                text_values.append(sql_literal(code_text))

    for alias in status_aliases:
        text_values.append(sql_literal(alias))

    unique_numeric_values = []
    unique_text_values = []
    seen = set()
    for value in numeric_values:
        if value not in seen:
            seen.add(value)
            unique_numeric_values.append(value)

    seen.clear()
    for value in text_values:
        if value not in seen:
            seen.add(value)
            unique_text_values.append(value)

    conditions = []
    if unique_numeric_values:
        conditions.append(f"{column_name} IN ({', '.join(unique_numeric_values)})")
    if unique_text_values:
        normalized_column = f"LOWER(TRIM(CAST(COALESCE({column_name}, '') AS CHAR)))"
        conditions.append(f"{normalized_column} IN ({', '.join(unique_text_values)})")

    if not conditions:
        return ""
    if len(conditions) == 1:
        return conditions[0]
    return "(" + " OR ".join(conditions) + ")"


def build_recipient_count_only_sql(account_id, user_query):
    """Build deterministic recipient count SQL in account scope."""
    account_id_lit = sql_literal(account_id)
    conditions = [f"account_id = {account_id_lit}"]

    status_condition = _build_recipient_status_sql_condition(
        "recipient_status",
        detect_requested_recipient_status_keys(user_query),
    )
    if status_condition:
        conditions.append(status_condition)

    where_sql = " AND ".join(conditions)
    return (
        "SELECT COUNT(DISTINCT recipient_id) AS total_records "
        "FROM core_recipients "
        f"WHERE {where_sql}"
    )


def detect_duplicate_name_request_field(user_query):
    """Detect whether duplicate check should run for first_name, last_name, or both."""
    text = normalize_intent_text(user_query)
    if not text:
        return "none"

    has_duplicate_term = any(
        term in text
        for term in (
            "duplicate",
            "duplicates",
            "duplicated",
            "repeat",
            "repeated",
            "same name",
            "same names",
        )
    )
    if not has_duplicate_term:
        return "none"

    has_first_name_term = any(
        term in text
        for term in (
            "first name",
            "first names",
            "firstname",
            "preferred first name",
            "preferred_first_name",
        )
    )
    has_last_name_term = any(
        term in text
        for term in (
            "last name",
            "last names",
            "lastname",
            "surname",
            "surnames",
            "family name",
            "family names",
        )
    )

    # Keep this recipient-scoped so package/table generic routes do not override it.
    has_recipient_context = any(
        term in text
        for term in (
            "recipient",
            "recipients",
            "member",
            "members",
            "name",
            "names",
        )
    )

    if has_first_name_term and has_last_name_term:
        return "both"
    if has_last_name_term:
        return "last_name"
    if has_first_name_term:
        return "first_name"
    if has_recipient_context:
        return "both"
    return "none"


def should_return_duplicate_name_all_columns(user_query):
    """Return True when user asks duplicate-name results with full row details."""
    text = normalize_intent_text(user_query)
    if not text:
        return False

    full_detail_markers = (
        "all columns",
        "all fields",
        "full details",
        "all details",
        "complete details",
        "select *",
    )
    return any(marker in text for marker in full_detail_markers)


def detect_duplicate_name_followup_contact_fields(user_query):
    """Detect requested contact fields for duplicate-name follow-up requests."""
    text = normalize_intent_text(user_query)
    if not text:
        return []

    asks_email = any(
        marker in text
        for marker in (
            "email",
            "emails",
            "mail id",
            "mail ids",
            "email id",
            "email ids",
            "e mail",
        )
    )
    asks_phone = any(
        marker in text
        for marker in (
            "cell",
            "cellphone",
            "cell phone",
            "mobile",
            "phone",
            "contact number",
            "contact numbers",
        )
    )

    requested_fields = []
    if asks_email:
        requested_fields.append("email")
    if asks_phone:
        requested_fields.append("cellphone")
    return requested_fields


def is_duplicate_name_contextual_followup_request(user_query, duplicate_context):
    """Return True for pronoun-based follow-up asks on prior duplicate-name result context."""
    text = normalize_intent_text(user_query)
    if not text or not isinstance(duplicate_context, dict):
        return False

    context_field = str(duplicate_context.get("name_field", "") or "").strip().lower()
    if context_field not in ("first_name", "last_name", "both"):
        return False

    requested_fields = detect_duplicate_name_followup_contact_fields(user_query)
    if not requested_fields:
        return False

    # If user asks a fresh explicit intent, do not hijack with duplicate-context follow-up.
    explicit_new_intent_terms = (
        "account billing",
        "billing",
        "credit card",
        "package",
        "packages",
        "tracking",
        "carrier",
        "delivered",
        "report",
        "table",
        "show me all",
        "list all",
        "core recipients",
        "core_recipients",
    )
    if any(term in text for term in explicit_new_intent_terms):
        return False

    has_reference = any(
        re.search(rf"\b{re.escape(token)}\b", text) is not None
        for token in (
            "their",
            "them",
            "those",
            "these",
            "same",
        )
    )
    return has_reference


def build_duplicate_name_followup_contact_sql(account_id, duplicate_name_field, contact_fields, row_limit=100):
    """Build deterministic SQL for contact list of recipients matched by duplicate-name scope."""
    account_id_lit = sql_literal(account_id)
    safe_limit = max(1, min(int(row_limit), 200)) if row_limit is not None else 100
    selected_contact_fields = [field for field in (contact_fields or []) if field in ("email", "cellphone")]
    if not selected_contact_fields:
        selected_contact_fields = ["email"]

    first_name_expr = (
        "COALESCE(NULLIF(TRIM(COALESCE(cr.preferred_first_name, cr.first_name)), ''), NULLIF(TRIM(cr.first_name), ''))"
    )
    last_name_expr = "NULLIF(TRIM(cr.last_name), '')"

    duplicate_first_subquery = (
        "SELECT "
        "COALESCE(NULLIF(TRIM(COALESCE(preferred_first_name, first_name)), ''), NULLIF(TRIM(first_name), '')) "
        "FROM core_recipients "
        f"WHERE account_id = {account_id_lit} "
        "GROUP BY COALESCE(NULLIF(TRIM(COALESCE(preferred_first_name, first_name)), ''), NULLIF(TRIM(first_name), '')) "
        "HAVING COUNT(*) > 1"
    )
    duplicate_last_subquery = (
        "SELECT NULLIF(TRIM(last_name), '') "
        "FROM core_recipients "
        f"WHERE account_id = {account_id_lit} "
        "GROUP BY NULLIF(TRIM(last_name), '') "
        "HAVING COUNT(*) > 1"
    )

    if duplicate_name_field == "last_name":
        duplicate_condition = f"{last_name_expr} IN ({duplicate_last_subquery})"
    elif duplicate_name_field == "both":
        duplicate_condition = (
            f"({first_name_expr} IN ({duplicate_first_subquery}) "
            f"OR {last_name_expr} IN ({duplicate_last_subquery}))"
        )
    else:
        duplicate_condition = f"{first_name_expr} IN ({duplicate_first_subquery})"

    presence_conditions = []
    for field_name in selected_contact_fields:
        presence_conditions.append(f"NULLIF(TRIM(COALESCE(cr.{field_name}, '')), '') IS NOT NULL")
    presence_sql = " OR ".join(presence_conditions)
    select_contact_sql = ", ".join(f"cr.{field_name}" for field_name in selected_contact_fields)

    return (
        "SELECT cr.recipient_id, cr.account_id, "
        "COALESCE(NULLIF(TRIM(COALESCE(cr.preferred_first_name, cr.first_name)), ''), NULLIF(TRIM(cr.first_name), '')) AS first_name, "
        "NULLIF(TRIM(cr.last_name), '') AS last_name, "
        f"{select_contact_sql} "
        "FROM core_recipients cr "
        f"WHERE cr.account_id = {account_id_lit} "
        f"AND ({presence_sql}) "
        f"AND {duplicate_condition} "
        "ORDER BY first_name ASC, last_name ASC, cr.recipient_id DESC "
        f"LIMIT {safe_limit}"
    )


def build_duplicate_name_sql(account_id, name_field, row_limit=100):
    """Build deterministic SQL for duplicate recipient name detection."""
    account_id_lit = sql_literal(account_id)
    safe_limit = max(1, min(int(row_limit), 200)) if row_limit is not None else 100

    first_name_expr = (
        "COALESCE(NULLIF(TRIM(COALESCE(preferred_first_name, first_name)), ''), NULLIF(TRIM(first_name), ''))"
    )
    last_name_expr = "NULLIF(TRIM(last_name), '')"

    if name_field == "last_name":
        return (
            "SELECT first_name, last_name, duplicate_count "
            "FROM ("
            "SELECT "
            "'-' AS first_name, "
            f"{last_name_expr} AS last_name, "
            "COUNT(*) AS duplicate_count "
            "FROM core_recipients "
            f"WHERE account_id = {account_id_lit} "
            f"GROUP BY {last_name_expr} "
            "HAVING last_name IS NOT NULL AND last_name <> '' AND COUNT(*) > 1"
            ") duplicates "
            "ORDER BY duplicate_count DESC, last_name ASC, first_name ASC "
            f"LIMIT {safe_limit}"
        )

    if name_field == "both":
        return (
            "SELECT first_name, last_name, duplicate_count "
            "FROM ("
            "SELECT "
            f"{first_name_expr} AS first_name, "
            "'-' AS last_name, "
            "COUNT(*) AS duplicate_count "
            "FROM core_recipients "
            f"WHERE account_id = {account_id_lit} "
            f"GROUP BY {first_name_expr} "
            "HAVING first_name IS NOT NULL AND first_name <> '' AND COUNT(*) > 1 "
            "UNION ALL "
            "SELECT "
            "'-' AS first_name, "
            f"{last_name_expr} AS last_name, "
            "COUNT(*) AS duplicate_count "
            "FROM core_recipients "
            f"WHERE account_id = {account_id_lit} "
            f"GROUP BY {last_name_expr} "
            "HAVING last_name IS NOT NULL AND last_name <> '' AND COUNT(*) > 1"
            ") duplicates "
            "ORDER BY duplicate_count DESC, first_name ASC, last_name ASC "
            f"LIMIT {safe_limit}"
        )

    return (
        "SELECT first_name, last_name, duplicate_count "
        "FROM ("
        "SELECT "
        f"{first_name_expr} AS first_name, "
        "'-' AS last_name, "
        "COUNT(*) AS duplicate_count "
        "FROM core_recipients "
        f"WHERE account_id = {account_id_lit} "
        f"GROUP BY {first_name_expr} "
        "HAVING first_name IS NOT NULL AND first_name <> '' AND COUNT(*) > 1"
        ") duplicates "
        "ORDER BY duplicate_count DESC, first_name ASC, last_name ASC "
        f"LIMIT {safe_limit}"
    )


def build_duplicate_name_details_sql(account_id, name_field, row_limit=100):
    """Build deterministic SQL returning full recipient rows for duplicate-name matches."""
    account_id_lit = sql_literal(account_id)
    safe_limit = max(1, min(int(row_limit), 200)) if row_limit is not None else 100

    first_name_expr = (
        "COALESCE(NULLIF(TRIM(COALESCE(cr.preferred_first_name, cr.first_name)), ''), NULLIF(TRIM(cr.first_name), ''))"
    )
    last_name_expr = "NULLIF(TRIM(cr.last_name), '')"

    duplicate_first_subquery = (
        "SELECT "
        "COALESCE(NULLIF(TRIM(COALESCE(preferred_first_name, first_name)), ''), NULLIF(TRIM(first_name), '')) "
        "FROM core_recipients "
        f"WHERE account_id = {account_id_lit} "
        "GROUP BY COALESCE(NULLIF(TRIM(COALESCE(preferred_first_name, first_name)), ''), NULLIF(TRIM(first_name), '')) "
        "HAVING COUNT(*) > 1"
    )
    duplicate_last_subquery = (
        "SELECT NULLIF(TRIM(last_name), '') "
        "FROM core_recipients "
        f"WHERE account_id = {account_id_lit} "
        "GROUP BY NULLIF(TRIM(last_name), '') "
        "HAVING COUNT(*) > 1"
    )

    if name_field == "last_name":
        duplicate_condition = f"{last_name_expr} IN ({duplicate_last_subquery})"
    elif name_field == "both":
        duplicate_condition = (
            f"({first_name_expr} IN ({duplicate_first_subquery}) "
            f"OR {last_name_expr} IN ({duplicate_last_subquery}))"
        )
    else:
        duplicate_condition = f"{first_name_expr} IN ({duplicate_first_subquery})"

    return (
        "SELECT cr.* "
        "FROM core_recipients cr "
        f"WHERE cr.account_id = {account_id_lit} "
        f"AND {duplicate_condition} "
        "ORDER BY cr.recipient_id DESC "
        f"LIMIT {safe_limit}"
    )


def _has_missing_contact_intent(text):
    """Return True when the query asks for recipients missing contact details."""
    if not text:
        return False
    text_compact = f" {text} "
    negative_markers = (
        " does not have ",
        " do not have ",
        " does note have ",
        " do note have ",
        " not have ",
        " not having ",
        " not having both ",
        " not have both ",
        " note having ",
        " note having both ",
        " note have ",
        " note have both ",
        " not with both ",
        " not with ",
        " dont have ",
        " without ",
        " missing ",
        " no email ",
        " no phone ",
        " no mobile ",
        " empty email ",
        " empty cellphone ",
        " empty cell phone ",
        " empty mobile ",
        " empty contact ",
        " empty phone ",
        " null email ",
        " null cellphone ",
        " null cell phone ",
        " null mobile ",
        " null contact ",
        " null phone ",
    )
    if any(marker in text_compact for marker in negative_markers):
        return True

    # Fallback dynamic pattern: catch phrasing like
    # "who do/does not|note|no have|has ... email/phone" after normalization.
    if re.search(r"\b(?:do|does)\s+(?:not|note|no)\s+(?:have|has)\b", text) is not None:
        return True

    # Catch direct forms such as "not/note having both cellphone and email".
    return re.search(r"\b(?:not|note)\s+(?:have|having)\b", text) is not None


def _requires_both_missing_contact_fields(text):
    """Return True when wording implies both email and cellphone must be missing."""
    if not text or not _has_missing_contact_intent(text):
        return False

    text_compact = f" {text} "
    has_email = " email " in text_compact
    has_phone = (
        " phone " in text_compact
        or " cellphone " in text_compact
        or " cell phone " in text_compact
        or " mobile " in text_compact
    )
    if not (has_email and has_phone):
        return False

    if " both " in text_compact:
        return True

    return " and " in text_compact and " or " not in text_compact


def _asks_cellphone_contact(text):
    """Return True when query asks for phone/cell/contact-number details."""
    if not text:
        return False
    return (
        "cell" in text
        or "phone" in text
        or "mobile" in text
        or "contact number" in text
        or "contact numbers" in text
        or "phone number" in text
        or "phone numbers" in text
    )


def build_recipient_directory_sql(account_id, user_query, row_limit=100):
    """Build deterministic recipient directory SQL in account scope."""
    text = normalize_intent_text(user_query)
    account_id_lit = sql_literal(account_id)
    safe_limit = None if row_limit is None else max(1, min(int(row_limit), 100))

    conditions = [f"cr.account_id = {account_id_lit}"]
    status_condition = _build_recipient_status_sql_condition(
        "cr.recipient_status",
        detect_requested_recipient_status_keys(text),
    )
    if status_condition:
        conditions.append(status_condition)
    asks_email = "email" in text
    asks_phone = _asks_cellphone_contact(text)
    missing_contact = _has_missing_contact_intent(text)

    if asks_email and asks_phone:
        if missing_contact:
            connector = "AND" if _requires_both_missing_contact_fields(text) else "OR"
            conditions.append(
                f"(NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NULL {connector} NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NULL)"
            )
        else:
            conditions.append("NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NOT NULL")
            conditions.append("NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NOT NULL")
    elif asks_email:
        if missing_contact:
            conditions.append("NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NULL")
        else:
            conditions.append("NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NOT NULL")
    elif asks_phone:
        if missing_contact:
            conditions.append("NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NULL")
        else:
            conditions.append("NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NOT NULL")

    where_sql = " AND ".join(conditions)
    sql_text = (
        "SELECT cr.recipient_id, cr.account_id, "
        "COALESCE(NULLIF(TRIM(CONCAT_WS(' ', NULLIF(COALESCE(cr.preferred_first_name, cr.first_name), ''), NULLIF(cr.last_name, ''))), ''), "
        "NULLIF(cr.email, ''), CONCAT('Recipient ', cr.recipient_id)) AS recipient_name, "
        "cr.first_name, cr.preferred_first_name, "
        "cr.last_name, cr.email, cr.cellphone, cr.recipient_status, cr.date_added "
        "FROM core_recipients cr "
        f"WHERE {where_sql} "
        "ORDER BY cr.recipient_id DESC"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def build_recipient_packages_join_sql(account_id, user_query, row_limit=100):
    """Build deterministic join SQL between track_packages and core_recipients."""
    text = normalize_intent_text(user_query)
    account_id_lit = sql_literal(account_id)
    safe_limit = None if row_limit is None else max(1, min(int(row_limit), 100))

    conditions = [
        f"tp.account_id = {account_id_lit}",
        f"cr.account_id = {account_id_lit}",
        "tp.recipient_id = cr.recipient_id",
    ]
    if "delivered" in text:
        conditions.append("tp.date_received IS NOT NULL")
    if "today" in text:
        conditions.append("DATE(tp.date_received) = CURDATE()")
    status_condition = _build_recipient_status_sql_condition(
        "cr.recipient_status",
        detect_requested_recipient_status_keys(text),
    )
    if status_condition:
        conditions.append(status_condition)
    asks_email = "email" in text
    asks_phone = _asks_cellphone_contact(text)
    missing_contact = _has_missing_contact_intent(text)

    if asks_email and asks_phone:
        if missing_contact:
            connector = "AND" if _requires_both_missing_contact_fields(text) else "OR"
            conditions.append(
                f"(NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NULL {connector} NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NULL)"
            )
        else:
            conditions.append("NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NOT NULL")
            conditions.append("NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NOT NULL")
    elif asks_email:
        if missing_contact:
            conditions.append("NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NULL")
        else:
            conditions.append("NULLIF(TRIM(COALESCE(cr.email, '')), '') IS NOT NULL")
    elif asks_phone:
        if missing_contact:
            conditions.append("NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NULL")
        else:
            conditions.append("NULLIF(TRIM(COALESCE(cr.cellphone, '')), '') IS NOT NULL")

    where_sql = " AND ".join(conditions)
    sql_text = (
        "SELECT tp.package_id, tp.account_id, tp.tracking_number, tp.shipping_carrier, "
        "tp.date_received, tp.recipient_id, "
        "COALESCE(NULLIF(TRIM(CONCAT_WS(' ', NULLIF(COALESCE(cr.preferred_first_name, cr.first_name), ''), NULLIF(cr.last_name, ''))), ''), "
        "NULLIF(cr.email, ''), CONCAT('Recipient ', cr.recipient_id)) AS recipient_name, "
        "cr.first_name, cr.preferred_first_name, "
        "cr.last_name, cr.email, cr.cellphone, cr.recipient_status "
        "FROM track_packages tp "
        "JOIN core_recipients cr "
        "ON tp.recipient_id = cr.recipient_id AND tp.account_id = cr.account_id "
        f"WHERE {where_sql} "
        "ORDER BY tp.package_id DESC"
    )
    if safe_limit is not None:
        sql_text += f" LIMIT {safe_limit}"
    return sql_text


def build_recipient_template_no_data_answer(user_query):
    """Build deterministic no-data message for recipient template paths."""
    status_keys = detect_requested_recipient_status_keys(user_query)
    if "active" in status_keys:
        return "No active recipients were found for this account."
    if "inactive" in status_keys:
        return "No inactive recipients were found for this account."
    if "future" in status_keys:
        return "No future recipients were found for this account."
    return "No matching recipient records were found for this account based on your request."


def _row_identity(row):
    """Build a stable identity for intersection across full/partial SQL row projections."""
    if not isinstance(row, dict):
        return None

    for key_group in (
        ("package_id",),
        ("tracking_number", "date_received", "recipient_id"),
        ("tracking_number", "recipient_name"),
    ):
        values = []
        ok = True
        for key in key_group:
            value = get_first_available_value(row, (key,))
            if value in (None, ""):
                ok = False
                break
            values.append(str(value).strip().lower())
        if ok:
            return "|".join(values)

    normalized = to_json_safe_rows([row])[0]
    return json.dumps(normalized, sort_keys=True, default=str)


def _format_value_for_table(key, value):
    """Normalize values for stable table rendering on UI."""
    value = _mask_sensitive_value(key, value)
    if value is None:
        return ""

    key_text = str(key).lower()
    if "date" in key_text:
        return format_date_to_mmddyyyy(value)

    if isinstance(value, bool):
        return "Yes" if value else "No"

    if isinstance(value, (int, float, str)):
        return value

    return str(value)


def _is_empty_cell(value):
    """Return True when a cell should be treated as empty for UI display."""
    if value is None:
        return True

    # Numeric zero values are valid data and must remain visible.
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return False

    text = str(value).strip().lower()
    if text in ("", "none", "null", "nan", "0000-00-00", "0000-00-00 00:00:00"):
        return True

    return False


def build_table_dataset(rows, limit=TABLE_VIEW_SAFE_LIMIT):
    """Build a safe, UI-friendly dataset with predictable keys and capped rows."""
    safe_limit = max(1, min(int(limit), TABLE_VIEW_SAFE_LIMIT))
    limited_rows = rows[:safe_limit]
    if not limited_rows:
        return []

    normalized_rows = []
    for row in limited_rows:
        normalized_row = {}
        for key, value in row.items():
            normalized_row[str(key)] = _format_value_for_table(key, value)
        normalized_rows.append(normalized_row)

    # Hide columns that are empty/null/zero across the whole result set.
    all_columns = []
    seen_columns = set()
    for row in normalized_rows:
        for key in row.keys():
            if key not in seen_columns:
                seen_columns.add(key)
                all_columns.append(key)

    visible_columns = []
    for key in all_columns:
        if any(not _is_empty_cell(row.get(key)) for row in normalized_rows):
            visible_columns.append(key)

    filtered_rows = []
    for row in normalized_rows:
        filtered_row = {key: row.get(key, "") for key in visible_columns}
        filtered_rows.append(filtered_row)

    return filtered_rows


def project_rows_to_requested_fields(rows, requested_fields):
    """Keep only explicitly requested fields from SQL rows when possible."""
    if not isinstance(rows, list) or not rows:
        return rows
    if not isinstance(requested_fields, list) or not requested_fields:
        return rows

    normalized_requested = [str(field).strip().lower() for field in requested_fields if str(field).strip()]
    if not normalized_requested:
        return rows

    projected_rows = []
    matched_any_field = False

    for row in rows:
        if not isinstance(row, dict):
            projected_rows.append(row)
            continue

        lowered_map = {str(key).strip().lower(): (key, value) for key, value in row.items()}
        projected_row = {}
        for requested in normalized_requested:
            if requested in lowered_map:
                original_key, value = lowered_map[requested]
                projected_row[str(original_key)] = value
                matched_any_field = True

        if projected_row:
            projected_rows.append(projected_row)
        else:
            projected_rows.append({})

    if not matched_any_field:
        return rows

    non_empty_rows = [row for row in projected_rows if isinstance(row, dict) and row]
    return non_empty_rows or rows


def detect_response_detail_mode_switch(user_query):
    """Detect whether user is asking to switch response detail mode."""
    text = normalize_intent_text(user_query)
    if not text:
        return None

    if any(re.search(pattern, text) for pattern in STRICT_DETAIL_ENABLE_PATTERNS):
        return "strict"
    if any(re.search(pattern, text) for pattern in STRICT_DETAIL_DISABLE_PATTERNS):
        return "rich"
    return None


def is_single_column_result(rows):
    """Return True when SQL result effectively contains a single column."""
    if not isinstance(rows, list) or not rows:
        return False
    first_row = rows[0]
    if not isinstance(first_row, dict) or len(first_row.keys()) != 1:
        return False
    return all(isinstance(row, dict) and len(row.keys()) == 1 for row in rows)


def build_single_column_text_answer(rows):
    """Build concise text response for single-column result sets."""
    if not is_single_column_result(rows):
        return None

    first_row = rows[0]
    column_name = str(next(iter(first_row.keys())))

    values = []
    seen = set()
    for row in rows:
        value = _mask_sensitive_value(column_name, row.get(column_name))
        value_text = "" if value is None else str(value).strip()
        if not value_text:
            continue
        lowered = value_text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        values.append(value_text)

    if not values:
        return f"No non-empty values were found for {column_name}."

    shown_values = values[:SINGLE_COLUMN_TEXT_MAX_VALUES]
    remaining = len(values) - len(shown_values)
    base = f"{column_name}: " + ", ".join(shown_values)
    if remaining > 0:
        return f"{base}. And {remaining} more value(s)."
    return base


def build_python_data_answer(user_query, rows, generated_sql=""):
    """Create deterministic answer text from SQL rows without LLM reconstruction."""
    display_rows = _mask_rows_for_display(rows) if isinstance(rows, list) else rows
    operation_profile = resolve_requested_operation(user_query)
    if operation_profile.get("mode") == "chart":
        if not display_rows:
            return build_no_data_fallback_from_query(user_query)
        return f"Generated chart data with {len(display_rows)} point(s)."

    explicit = build_explicit_answer(user_query, display_rows, generated_sql)
    if explicit:
        return explicit

    if not display_rows:
        return build_no_data_fallback_from_query(user_query)

    if query_requests_table_view(user_query):
        return "Showing the requested data in table format."

    single_column_answer = build_single_column_text_answer(display_rows)
    if single_column_answer:
        return single_column_answer

    return f"Here are the data fetched for your request. I found {len(display_rows)} matching record(s)."


def harmonize_answer_with_display_mode(user_query, answer, rows, display_mode, chart_payload):
    """Keep answer text consistent with rendered output mode."""
    rows_list = rows if isinstance(rows, list) else []
    operation_profile = resolve_requested_operation(user_query)
    chart_requested = bool(operation_profile.get("mode") == "chart" or operation_profile.get("explicit_chart"))

    if display_mode == "chart" and isinstance(chart_payload, dict):
        if chart_payload.get("render_error"):
            requested_type = str(chart_payload.get("chart_type") or "chart").strip().lower()
            return f"I could not render the requested {requested_type} chart for this data."

        point_count = int(chart_payload.get("point_count", 0) or 0)
        if point_count <= 0:
            labels = chart_payload.get("labels") if isinstance(chart_payload.get("labels"), list) else []
            values = chart_payload.get("values") if isinstance(chart_payload.get("values"), list) else []
            point_count = len(values) or len(labels) or len(rows_list)
        return f"Generated chart data with {point_count} point(s)."

    if chart_requested:
        return f"I could not render a chart for this request, so here is the data in table format ({len(rows_list)} record(s))."

    return answer


def is_followup_records_request(user_query):
    """Detect follow-up questions asking to show previously referenced records."""
    text = str(user_query or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return any(re.search(pattern, text) for pattern in FOLLOWUP_RECORD_PATTERNS)


def is_sensitive_query(user_query):
    """Return True when user asks for restricted sensitive credentials/secrets."""
    text = normalize_intent_text(user_query)
    return any(re.search(pattern, text) for pattern in SENSITIVE_QUERY_PATTERNS)


@app.route("/", methods=["GET", "POST"])
def login_page():
    mark_step("login_page_enter")
    response_status = None
    response_body = None
    db_rows = []

    if request.method == "POST":
        mark_step("login_post_received")
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()
        mark_step("login_form_parsed", username_present=bool(username), password_present=bool(password))

        payload = {
            "username": username,
            "password": password,
            "session_timedout": "7 days",
            "device_unique_id": "Postman",
            "app_version": "7.8",
        }

        try:
            api_call_started_at = time.perf_counter()
            api_response = requests.post(LOGIN_API_URL, json=payload, timeout=20)
            response_status = api_response.status_code
            response_body = api_response.text
            mark_step(
                "login_api_completed",
                status=response_status,
                duration_s=f"{(time.perf_counter() - api_call_started_at):.3f}",
            )
            logger.info("Login API call completed with status=%s", response_status)

            if response_status == 200:
                try:
                    response_json = api_response.json()
                    mark_step("login_api_json_parsed", api_status=response_json.get("api_status"))
                except ValueError:
                    response_json = None
                    mark_step("login_api_json_parse_failed")

                if response_json and response_json.get("api_status") == "success":
                    account_id = (
                        response_json.get("account_id")
                        or response_json.get("account _id")
                        or response_json.get("accountId")
                    )
                    mark_step("login_api_success", account_id=account_id)
                    logger.info("Login API success response: %s", response_json)

                    if account_id:
                        session["account_id"] = str(account_id)
                        set_current_account_id(account_id)
                        mark_step("login_session_account_set", account_id=account_id)

                        fetch_started_at = time.perf_counter()
                        db_rows, track_fetch_status = fetch_track_packages_data(
                            account_id,
                            include_status=True,
                        )
                        mark_step(
                            "login_track_packages_fetched",
                            rows=len(db_rows),
                            status=track_fetch_status,
                            duration_s=f"{(time.perf_counter() - fetch_started_at):.3f}",
                        )

                        session["account_data"] = db_rows
                        session["account_data_fetch_status"] = track_fetch_status
                        session["account_data_version"] = str(time.time_ns())
                        mark_step("login_session_data_set", rows=len(db_rows), status=track_fetch_status)
                        logger.info(
                            "track_packages(account_id=%s) fetch completed rows=%s status=%s",
                            account_id,
                            len(db_rows),
                            track_fetch_status,
                        )
                        if track_fetch_status != "ok":
                            logger.warning(
                                "Proceeding to chatbot with empty/stale account_data due to fetch status=%s",
                                track_fetch_status,
                            )
                        for index, row in enumerate(db_rows, start=1):
                            logger.info(
                                "track_packages row %s (login account_id=%s): %s",
                                index,
                                account_id,
                                row,
                            )
                        mark_step("login_redirect_chatbot")
                        return redirect(url_for("chatbot_page"))
                    else:
                        mark_step("login_account_id_missing")
                        logger.warning("account_id missing in successful login response.")
        except requests.RequestException as error:
            response_status = "Request failed"
            response_body = str(error)
            mark_step("login_api_request_exception", error=str(error))
            logger.error("Login API request failed: %s", error)

    mark_step("login_render_template")
    return render_template(
        "login.html",
        response_status=response_status,
        response_body=response_body,
        db_rows=db_rows,
    )


@app.route("/chatbot", methods=["GET"])
def chatbot_page():
    mark_step("chatbot_page_enter")
    account_id = session.get("account_id")
    if not account_id:
        mark_step("chatbot_page_missing_account_id_redirect")
        return redirect(url_for("login_page"))

    account_data = session.get("account_data")
    if not isinstance(account_data, list):
        account_data = []
        session["account_data"] = account_data

    account_data_fetch_status = str(session.get("account_data_fetch_status", "unknown") or "unknown")

    mark_step(
        "chatbot_page_render",
        account_data_count=len(account_data),
        account_data_fetch_status=account_data_fetch_status,
    )
    return render_template(
        "chatbot.html",
        account_id=account_id,
        account_data_count=len(account_data),
        account_data_fetch_status=account_data_fetch_status,
    )


@app.route("/chatbot/ask", methods=["POST"])
def chatbot_ask():
    mark_step("chatbot_ask_enter")
    g.chatbot_response_cache_key = ""
    account_id = session.get("account_id")
    if not account_id:
        mark_step("chatbot_ask_unauthorized")
        return jsonify({"error": "Session expired. Please login again."}), 401

    payload = request.get_json(silent=True) or {}
    mark_step("chatbot_ask_payload_parsed")
    user_query = str(payload.get("message", "")).strip()
    if not user_query:
        mark_step("chatbot_ask_missing_message")
        return jsonify({"error": "message is required"}), 400
    mark_step("chatbot_ask_message_ready", query_len=len(user_query))

    detail_mode_switch = detect_response_detail_mode_switch(user_query)
    if detail_mode_switch:
        session["response_detail_mode"] = detail_mode_switch
        mark_step("chatbot_ask_response_detail_mode_switched", mode=detail_mode_switch)
    response_detail_mode = str(session.get("response_detail_mode", "rich") or "rich").strip().lower()
    strict_detail_mode = response_detail_mode == "strict"
    operation_profile = resolve_requested_operation(user_query)
    mark_step(
        "chatbot_ask_operation_profile",
        mode=operation_profile.get("mode"),
        chart_full_data=bool(operation_profile.get("chart_full_data")),
    )

    pending_action = session.get("pending_action")
    cache_skip = should_skip_query_response_cache(user_query, pending_action=pending_action)
    if QUERY_RESPONSE_CACHE_ENABLED and not cache_skip:
        account_data_version = session.get("account_data_version", "0")
        cache_key = _build_query_response_cache_key(
            account_id,
            user_query,
            response_detail_mode,
            account_data_version,
        )
        cached_response = _get_cached_query_response(cache_key)
        if cached_response:
            cached_display = str(cached_response.get("display", "") or "")
            cached_chart = cached_response.get("chart") if isinstance(cached_response.get("chart"), dict) else None
            cached_rows = cached_response.get("rows") if isinstance(cached_response.get("rows"), list) else []
            if cached_rows:
                session["last_result_rows"] = to_json_safe_rows(cached_rows)
            session["last_user_query"] = user_query
            update_chart_context(cached_display, cached_chart)
            mark_step("chatbot_ask_response_cache_hit")
            return jsonify(cached_response)
        g.chatbot_response_cache_key = cache_key

    if is_sensitive_query(user_query):
        answer = SENSITIVE_QUERY_RESPONSE
        log_chat_interaction(
            account_id,
            user_query,
            "",
            "restricted_sensitive_query",
            0,
            answer,
        )
        mark_step("chatbot_ask_sensitive_query_blocked")
        return jsonify({"answer": answer, "rows": [], "status": "restricted_sensitive_query"})

    last_chart_payload = session.get("last_chart_payload")
    if (
        isinstance(last_chart_payload, dict)
        and last_chart_payload
        and is_chart_followup_question(user_query)
        and not is_ocr_success_breakdown_request(user_query)
    ):
        answer = answer_from_last_chart(user_query, last_chart_payload)
        if answer:
            log_chat_interaction(
                account_id,
                user_query,
                "LAST_CHART_CONTEXT",
                "ok:chart_followup_answered",
                int(last_chart_payload.get("point_count", 0) or 0),
                answer,
            )
            mark_step("chatbot_ask_chart_followup_answered")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

    if pending_action and pending_action.get("type") == "show_records_confirmation":
        mark_step("chatbot_ask_pending_confirmation")
        normalized = user_query.strip().lower()

        if normalized in ("yes", "y", "ok", "okay"):
            rows = pending_action.get("rows", [])
            table_rows = build_table_dataset(rows, limit=TABLE_VIEW_SAFE_LIMIT)
            count = pending_action.get("count", len(table_rows))
            answer = f"Here are the data fetched for your request ({count} record(s))."
            if operation_profile.get("mode") == "text":
                default_display = "text"
            elif operation_profile.get("mode") == "table":
                default_display = "table"
            else:
                default_display = "key_value"
            display_mode, response_rows, chart_payload = resolve_visual_response(
                user_query,
                table_rows,
                default_display,
            )
            session.pop("pending_action", None)
            log_chat_interaction(
                account_id,
                user_query,
                "SESSION_DATA_ONLY_CONFIRMATION",
                "confirmation_yes_show_rows",
                len(table_rows),
                answer,
            )
            mark_step("chatbot_ask_confirmation_yes_return", rows=len(table_rows))
            update_chart_context(display_mode, chart_payload)
            return jsonify(
                {
                    "answer": answer,
                    "rows": response_rows,
                    "display": display_mode,
                    "chart": chart_payload,
                    "status": "ok",
                }
            )

        if normalized == "no":
            answer = REQUEST_COMPLETED_RESPONSE
            session.pop("pending_action", None)
            log_chat_interaction(
                account_id,
                user_query,
                "SESSION_DATA_ONLY_CONFIRMATION",
                "confirmation_no_completed",
                0,
                answer,
            )
            mark_step("chatbot_ask_confirmation_no_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        answer = CONFIRMATION_INVALID_RESPONSE
        log_chat_interaction(
            account_id,
            user_query,
            "SESSION_DATA_ONLY_CONFIRMATION",
            "confirmation_invalid_input",
            0,
            answer,
        )
        mark_step("chatbot_ask_confirmation_invalid_return")
        return jsonify({"answer": answer, "rows": [], "status": "awaiting_confirmation"})

    # If user asks to change representation (for example "show in table format"),
    # reuse the last fetched result instead of rerouting to greeting/help.
    # Explicit table-name requests must bypass this shortcut and execute fresh SQL.
    last_rows = session.get("last_result_rows", [])
    explicit_followup_tables = []
    if isinstance(last_rows, list) and last_rows:
        followup_schema_map = fetch_schema_metadata_for_chatbot(force_refresh=False)
        explicit_followup_tables = detect_explicit_query_tables(
            user_query,
            followup_schema_map,
            max_tables=2,
        )

    if (
        isinstance(last_rows, list)
        and last_rows
        and is_format_only_followup_request(user_query)
        and not explicit_followup_tables
    ):
        requested_limit = extract_requested_row_limit(user_query, max_limit=TABLE_VIEW_SAFE_LIMIT)
        reusable_rows = last_rows[:requested_limit] if requested_limit else last_rows
        table_rows = build_table_dataset(reusable_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        if operation_profile.get("mode") == "text":
            default_display = "text"
        elif operation_profile.get("mode") == "table":
            default_display = "table"
        else:
            default_display = "key_value"
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            default_display,
        )
        logger.info(
            "FOLLOWUP_FORMAT | query=%s | default_display=%s | resolved_display=%s | rows=%s | chart_payload=%s",
            normalize_generated_sql_for_log(user_query),
            default_display,
            display_mode,
            len(table_rows),
            bool(chart_payload),
        )
        answer = f"Here are the data fetched for your request ({len(table_rows)} record(s))."
        mark_step("chatbot_ask_display_mode_followup", display=display_mode, rows=len(table_rows))
        update_chart_context(display_mode, chart_payload)
        log_chat_interaction(
            account_id,
            user_query,
            "FOLLOWUP_FORMAT_FROM_CONTEXT",
            "ok:followup_display_mode",
            len(table_rows),
            answer,
        )
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_followup_records_request(user_query):
        mark_step("chatbot_ask_followup_detected")
        last_rows = session.get("last_result_rows", [])
        if isinstance(last_rows, list) and last_rows:
            requested_limit = extract_requested_row_limit(user_query, max_limit=TABLE_VIEW_SAFE_LIMIT)
            reusable_rows = last_rows[:requested_limit] if requested_limit else last_rows
            table_rows = build_table_dataset(reusable_rows, limit=TABLE_VIEW_SAFE_LIMIT)
            count = len(table_rows)
            answer = f"Here are the data fetched for your request ({count} record(s))."
            if operation_profile.get("mode") == "text":
                default_display = "text"
            elif operation_profile.get("mode") == "table":
                default_display = "table"
            else:
                default_display = "key_value"
            display_mode, response_rows, chart_payload = resolve_visual_response(
                user_query,
                table_rows,
                default_display,
            )
            log_chat_interaction(
                account_id,
                user_query,
                "FOLLOWUP_FROM_CONTEXT",
                "ok:followup_records",
                count,
                answer,
            )
            mark_step("chatbot_ask_followup_rows_return", rows=count)
            update_chart_context(display_mode, chart_payload)
            return jsonify(
                {
                    "answer": answer,
                    "rows": response_rows,
                    "display": display_mode,
                    "chart": chart_payload,
                    "status": "ok",
                }
            )

        answer = FOLLOWUP_CONTEXT_MISSING_RESPONSE
        log_chat_interaction(
            account_id,
            user_query,
            "FOLLOWUP_FROM_CONTEXT",
            "followup_context_missing",
            0,
            answer,
        )
        mark_step("chatbot_ask_followup_missing_context_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    last_user_query = str(session.get("last_user_query", "") or "")
    delivery_date_intent = analyze_delivery_date_intent(user_query, last_user_query)
    if delivery_date_intent.get("normalized_query") != normalize_intent_text(user_query):
        mark_step(
            "chatbot_ask_dynamic_followup_context_applied",
            mode=delivery_date_intent.get("mode", "none"),
        )

    if is_report_request(user_query):
        mark_step("chatbot_ask_report_mode_detected")
        session_rows = session.get("account_data", [])
        if not isinstance(session_rows, list) or not session_rows:
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                "REPORT_MODE",
                "report_session_data_missing",
                0,
                answer,
            )
            return jsonify({"answer": answer, "rows": [], "status": "session_data_missing"})

        report_limit = extract_requested_row_limit(user_query, max_limit=REPORT_TABLE_DEFAULT_LIMIT)
        if report_limit is None:
            report_limit = REPORT_TABLE_DEFAULT_LIMIT
        report_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        report_query_limit = None if report_visual_full_data else report_limit
        recipient_accuracy_mode = should_use_recipient_accuracy_path(user_query)
        requested_top_recipient_limit = (
            extract_requested_row_limit(user_query, max_limit=100)
            or extract_ranked_limit_from_raw_query(user_query, max_limit=100)
        )
        if report_visual_full_data and requested_top_recipient_limit is None:
            top_recipient_limit = None
        else:
            top_recipient_limit = requested_top_recipient_limit or TOP_RECIPIENT_DEFAULT_LIMIT

        report_sql = ""
        if is_ocr_success_breakdown_request(user_query):
            report_sql = build_ocr_success_breakdown_sql(account_id)
        elif recipient_accuracy_mode and is_top_recipient_request(user_query):
            report_sql = build_recipient_count_sql(account_id, user_query, row_limit=top_recipient_limit)
        elif recipient_accuracy_mode and is_recipient_wise_count_request(user_query):
            report_sql = build_recipient_wise_count_sql(account_id, user_query, row_limit=report_query_limit)
        elif not USE_DYNAMIC_SELECTOR_ONLY and is_account_billing_request(user_query):
            report_sql = build_account_billing_sql(account_id, user_query, row_limit=report_query_limit)
        elif not USE_DYNAMIC_SELECTOR_ONLY and is_latest_packages_request(user_query):
            report_sql = build_latest_packages_sql(
                account_id,
                row_limit=report_query_limit,
                schema_columns=TRACK_PACKAGES_COLUMNS,
            )
        elif not USE_DYNAMIC_SELECTOR_ONLY and delivery_date_intent.get("mode") == "peak":
            report_sql = build_peak_delivered_date_sql(account_id)
        elif not USE_DYNAMIC_SELECTOR_ONLY and delivery_date_intent.get("mode") in ("latest_date", "dates"):
            report_sql = build_delivered_dates_sql(account_id, limit=report_query_limit)
        elif recipient_accuracy_mode:
            recipient_template_intent = detect_recipient_template_intent(user_query)
            if recipient_template_intent == "recipient_count":
                report_sql = build_recipient_count_only_sql(account_id, user_query)
            elif recipient_template_intent == "recipient_directory":
                report_sql = build_recipient_directory_sql(account_id, user_query, row_limit=report_query_limit)
            elif recipient_template_intent == "recipient_packages_join":
                report_sql = build_recipient_packages_join_sql(account_id, user_query, row_limit=report_query_limit)
        elif not USE_DYNAMIC_SELECTOR_ONLY:
            recipient_template_intent = detect_recipient_template_intent(user_query)
            if recipient_template_intent == "recipient_count":
                report_sql = build_recipient_count_only_sql(account_id, user_query)
            elif recipient_template_intent == "recipient_directory":
                report_sql = build_recipient_directory_sql(account_id, user_query, row_limit=report_query_limit)
            elif recipient_template_intent == "recipient_packages_join":
                report_sql = build_recipient_packages_join_sql(account_id, user_query, row_limit=report_query_limit)

        if not report_sql:
            schema_map = fetch_schema_metadata_for_chatbot()
            prompt_tables, prompt_table_source, explicit_table_mode = select_prompt_tables_for_query(
                user_query,
                schema_map,
                max_tables=3,
            )
            if _is_ambiguous_marker(prompt_table_source) and len(prompt_tables) > 1:
                answer = build_table_disambiguation_response(prompt_tables)
                log_chat_interaction(
                    account_id,
                    user_query,
                    "",
                    f"insufficient_intent:{prompt_table_source}",
                    0,
                    answer,
                )
                mark_step("chatbot_ask_report_table_ambiguous")
                return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})

            direct_table_name = ""
            direct_table_reason = "none"
            force_direct_from_exact_prompt = (
                len(prompt_tables) == 1
                and prompt_table_source in EXACT_TABLE_PROMPT_SOURCES
            )
            force_direct_from_table_name_style = (
                len(prompt_tables) == 1
                and _is_table_name_style_request(user_query, _get_runtime_supported_tables(schema_map))
            )

            if TABLE_DIRECT_FAST_PATH_ENABLED and (
                _is_simple_table_direct_request(user_query)
                or force_direct_from_exact_prompt
                or force_direct_from_table_name_style
            ):
                if force_direct_from_exact_prompt or force_direct_from_table_name_style:
                    direct_table_name = prompt_tables[0]
                    direct_table_reason = f"prompt_source:{prompt_table_source}"
                else:
                    direct_table_name, direct_table_reason = detect_direct_target_table(
                        user_query,
                        schema_map,
                        prompt_tables=prompt_tables,
                        prompt_table_source=prompt_table_source,
                    )
                if not direct_table_name and _is_ambiguous_marker(direct_table_reason):
                    ambiguity_candidates = prompt_tables if prompt_tables else detect_best_query_tables(
                        user_query,
                        schema_map,
                        max_tables=5,
                    )
                    answer = build_table_disambiguation_response(ambiguity_candidates)
                    log_chat_interaction(
                        account_id,
                        user_query,
                        "",
                        f"insufficient_intent:{direct_table_reason}",
                        0,
                        answer,
                    )
                    mark_step("chatbot_ask_report_direct_table_ambiguous")
                    return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})

                if direct_table_name:
                    report_sql = build_direct_table_fast_sql(
                        account_id,
                        user_query,
                        direct_table_name,
                        schema_map,
                        row_limit=report_query_limit,
                    )
                    if report_sql:
                        log_routing_debug(
                            account_id,
                            user_query,
                            intent,
                            "direct_sql",
                            "report_direct_table_fast_path",
                            extra=(
                                f"table={direct_table_name}; reason={direct_table_reason}; "
                                f"source={prompt_table_source}; tables={','.join(prompt_tables)}"
                            ),
                        )

            if report_sql:
                report_sql = normalize_generated_sql_for_log(report_sql)

        if not report_sql:
            schema_text = build_supported_schema_text(schema_map, supported_tables=prompt_tables)
            allowed_columns_text = get_allowed_columns_text(schema_map, supported_tables=prompt_tables)
            rewritten_report_query = rewrite_user_query_for_sql(user_query)
            report_sql_timeout, report_sql_timeout_policy = compute_dynamic_sql_timeout(
                rewritten_report_query,
                prompt_tables,
                explicit_table_mode=explicit_table_mode,
            )
            report_sql = _get_cached_sql_generation(account_id, rewritten_report_query, prompt_tables)
            report_sql_cache_hit = bool(report_sql)
            if not report_sql_cache_hit:
                report_sql = call_qwen(
                    build_sql_prompt(
                        rewritten_report_query,
                        account_id,
                        schema_text,
                        allowed_columns_text=allowed_columns_text,
                        supported_tables=prompt_tables,
                    ),
                    timeout_seconds=report_sql_timeout,
                )
                _set_cached_sql_generation(account_id, rewritten_report_query, prompt_tables, report_sql)
            report_sql = normalize_generated_sql_for_log(report_sql)
            if report_sql and not (explicit_table_mode and EXPLICIT_TABLE_SKIP_SECONDARY_LLM):
                mismatch_reason = detect_sql_intent_mismatch_reason(user_query, report_sql)
                if mismatch_reason:
                    repaired_report_sql = call_qwen(
                        build_sql_intent_alignment_prompt(
                            user_query,
                            report_sql,
                            account_id,
                            schema_text,
                            supported_tables=prompt_tables,
                        ),
                        timeout_seconds=SECONDARY_QWEN_TIMEOUT,
                    )
                    repaired_report_sql = normalize_generated_sql_for_log(repaired_report_sql)
                    if repaired_report_sql:
                        report_sql = repaired_report_sql
            if report_sql:
                scope_ok, _ = is_supported_tables_account_scoped(report_sql, account_id)
                if not scope_ok:
                    report_sql = ""
            if not report_sql:
                if explicit_table_mode and prompt_tables:
                    report_sql = (
                        f"SELECT * FROM {prompt_tables[0]} "
                        f"WHERE account_id = {sql_literal(account_id)}"
                    )
                    if report_query_limit is not None:
                        report_sql += f" LIMIT {max(1, int(report_query_limit))}"
                else:
                    report_sql, report_fallback_source = build_backend_fallback_sql(user_query, account_id, schema_map)
                    if not report_sql and str(report_fallback_source).startswith("fallback_no_"):
                        candidate_tables = detect_prompt_table_candidates(user_query, schema_map, max_tables=6)
                        if len(candidate_tables) == 1:
                            report_sql, candidate_status = build_account_scoped_candidate_sql(
                                candidate_tables[0],
                                account_id,
                                schema_map,
                                row_limit=report_query_limit if isinstance(report_query_limit, int) else 100,
                            )
                            if report_sql:
                                report_fallback_source = f"candidate_select:{candidate_tables[0]}"
                            else:
                                report_fallback_source = f"fallback_no_candidate_sql:{candidate_status}"
                        if report_sql:
                            report_sql = normalize_generated_sql_for_log(report_sql)
                        else:
                            answer = (
                                build_table_disambiguation_response(candidate_tables)
                                if len(candidate_tables) > 1
                                else INTENT_CLARIFICATION_RESPONSE
                            )
                            log_chat_interaction(
                                account_id,
                                user_query,
                                "",
                                f"insufficient_intent:{report_fallback_source}",
                                0,
                                answer,
                            )
                            mark_step("chatbot_ask_report_insufficient_intent")
                            return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})

            log_routing_debug(
                account_id,
                user_query,
                intent,
                QWEN_MODEL,
                "report_prompt_tables",
                extra=(
                    f"source={prompt_table_source}; explicit_table_mode={explicit_table_mode}; "
                    f"sql_cache_hit={report_sql_cache_hit}; timeout_s={report_sql_timeout}; "
                    f"timeout_policy={report_sql_timeout_policy}; tables={','.join(prompt_tables)}"
                ),
            )

        report_exec_started_at = time.perf_counter()
        query_table_ok, query_table_reason = validate_query_table_relevance(user_query, report_sql)
        if not query_table_ok:
            answer = QUERY_REPHRASE_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                report_sql,
                f"query_table_mismatch:{query_table_reason}",
                0,
                answer,
            )
            mark_step("chatbot_ask_report_query_table_mismatch")
            return jsonify({"answer": answer, "rows": [], "status": "query_table_mismatch"})

        explicit_table_ok, explicit_table_reason, explicit_tables = validate_explicit_table_alignment(
            user_query,
            report_sql,
            schema_map,
        )
        if not explicit_table_ok:
            answer = QUERY_REPHRASE_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                report_sql,
                f"query_table_mismatch:{explicit_table_reason}",
                0,
                answer,
            )
            mark_step("chatbot_ask_report_explicit_table_mismatch")
            return jsonify({"answer": answer, "rows": [], "status": "query_table_mismatch"})

        report_schema_map = fetch_schema_metadata_for_chatbot(force_refresh=False)
        report_rows, report_status, report_source = execute_query_with_dataset_fallback(
            report_sql,
            account_id,
            report_schema_map,
            max_rows=report_query_limit,
        )
        mark_step(
            "chatbot_ask_report_sql_executed",
            rows=len(report_rows),
            sql_status=report_status,
            source=report_source,
            duration_s=f"{(time.perf_counter() - report_exec_started_at):.3f}",
        )

        if report_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                report_sql,
                f"report_sql_execution_failed:{report_status}",
                0,
                answer,
            )
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not report_rows:
            if explicit_tables:
                answer = build_explicit_table_no_data_response(explicit_tables)
            else:
                answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                report_sql,
                "report_no_rows",
                0,
                answer,
            )
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(report_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query
        display_mode, response_rows, chart_payload = resolve_report_view_response(
            user_query,
            table_rows,
            chart_source_rows=report_rows,
        )
        report_payload = build_report_payload(
            user_query,
            report_sql,
            table_rows,
            display_mode,
            chart_payload,
        )
        update_chart_context(display_mode, chart_payload)
        answer = (
            f"Generated report successfully with {len(table_rows)} record(s). "
            f"View mode: {display_mode}."
        )
        log_chat_interaction(
            account_id,
            user_query,
            report_sql,
            "ok:report_generated",
            len(table_rows),
            answer,
        )
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "report": report_payload,
                "status": "ok",
            }
        )

    classify_started_at = time.perf_counter()
    simple_direct_request = _is_simple_table_direct_request(user_query)
    if simple_direct_request:
        intent = "db"
    else:
        intent = classify_user_intent(user_query)
    mark_step(
        "chatbot_ask_intent_classified",
        intent=intent,
        fast_path=simple_direct_request,
        duration_s=f"{(time.perf_counter() - classify_started_at):.3f}",
    )

    if intent in ("help", "out_of_scope") and delivery_date_intent.get("mode") in ("peak", "dates"):
        intent = "db"
        mark_step("chatbot_ask_intent_overridden_to_db", reason="dynamic_delivery_date_intent")

    log_routing_debug(account_id, user_query, intent, "router", "intent_detected")
    if intent == "account_id":
        answer = f"The account id is {account_id}."
        log_routing_debug(account_id, user_query, intent, "none", "direct_account_id")
        log_chat_interaction(
            account_id,
            user_query,
            "",
            "direct_account_id",
            0,
            answer,
        )
        mark_step("chatbot_ask_account_id_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    if intent == "greeting":
        log_routing_debug(account_id, user_query, intent, "local_fast_path", "greeting_reply")
        greeting_replies = get_dynamic_greeting_replies()
        last_greeting = str(session.get("last_greeting_reply", "") or "")

        if len(greeting_replies) > 1 and last_greeting in greeting_replies:
            candidates = [reply for reply in greeting_replies if reply != last_greeting]
        else:
            candidates = greeting_replies

        answer = random.choice(candidates or greeting_replies)
        session["last_greeting_reply"] = answer
        mark_step("chatbot_ask_greeting_local")
        log_chat_interaction(
            account_id,
            user_query,
            "",
            "greeting",
            0,
            answer,
        )
        mark_step("chatbot_ask_greeting_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    if intent == "out_of_scope":
        log_routing_debug(account_id, user_query, intent, "local_fast_path", "out_of_scope_reply")
        answer = OUT_OF_DB_RESPONSE
        mark_step("chatbot_ask_out_scope_local")
        log_chat_interaction(
            account_id,
            user_query,
            "",
            "out_of_scope",
            0,
            answer,
        )
        mark_step("chatbot_ask_out_scope_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    if intent == "help":
        log_routing_debug(account_id, user_query, intent, "local_fast_path", "help_reply")
        answer = (
            "I can help with your account package data. "
            "Ask about tracking numbers, delivered dates, package counts, or recipient-wise summaries. "
            "For chart output, include words like 'show chart', 'plot', or 'graph'. "
            "For text-only output, include words like 'text only', 'summary only', or 'without table'. "
            "For report mode, include words like 'generate report' or 'show report'."
        )
        mark_step("chatbot_ask_help_local")
        log_chat_interaction(
            account_id,
            user_query,
            "",
            "help",
            0,
            answer,
        )
        mark_step("chatbot_ask_help_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    session_rows = session.get("account_data", [])
    mark_step("chatbot_ask_session_data_loaded", rows=len(session_rows) if isinstance(session_rows, list) else 0)
    if not isinstance(session_rows, list) or not session_rows:
        refetch_started_at = time.perf_counter()
        refreshed_rows, refreshed_status = fetch_track_packages_data(account_id, include_status=True)
        mark_step(
            "chatbot_ask_session_data_refetched",
            rows=len(refreshed_rows),
            status=refreshed_status,
            duration_s=f"{(time.perf_counter() - refetch_started_at):.3f}",
        )

        if isinstance(refreshed_rows, list) and refreshed_rows:
            session_rows = refreshed_rows
            session["account_data"] = refreshed_rows
            session["account_data_fetch_status"] = refreshed_status
            session["account_data_version"] = str(time.time_ns())
            mark_step("chatbot_ask_session_data_refetch_success", rows=len(refreshed_rows))
        else:
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                "SESSION_DATA_ONLY",
                f"session_data_missing_refetch_status:{refreshed_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_session_data_missing_return", status=refreshed_status)
            return jsonify({"answer": answer, "rows": [], "status": "session_data_missing"})

    ocr_record_scope = detect_ocr_triggered_records_scope(user_query)
    if ocr_record_scope != "none":
        requested_limit = extract_requested_row_limit(user_query, max_limit=2000)
        ocr_records_limit = requested_limit if requested_limit is not None else 100
        ocr_records_sql = build_ocr_triggered_records_sql(
            account_id,
            trigger_scope=ocr_record_scope,
            row_limit=ocr_records_limit,
        )
        ocr_records_exec_started_at = time.perf_counter()
        ocr_records_rows, ocr_records_status = execute_read_only_sql_for_chatbot(
            ocr_records_sql,
            max_rows=ocr_records_limit,
        )
        mark_step(
            "chatbot_ask_ocr_triggered_records_sql_executed",
            rows=len(ocr_records_rows),
            sql_status=ocr_records_status,
            scope=ocr_record_scope,
            duration_s=f"{(time.perf_counter() - ocr_records_exec_started_at):.3f}",
        )

        if ocr_records_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                ocr_records_sql,
                f"ocr_triggered_records_sql_failed:{ocr_records_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_ocr_triggered_records_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not ocr_records_rows:
            if ocr_record_scope == "not_triggered":
                answer = "No OCR not-triggered records were found for this account."
            elif ocr_record_scope == "triggered":
                answer = "No OCR triggered records were found for this account."
            else:
                answer = "No OCR-related records were found for this account."
            log_chat_interaction(
                account_id,
                user_query,
                ocr_records_sql,
                f"ocr_triggered_records_no_rows:{ocr_record_scope}",
                0,
                answer,
            )
            mark_step("chatbot_ask_ocr_triggered_records_no_rows_return", scope=ocr_record_scope)
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(ocr_records_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        if ocr_record_scope == "not_triggered":
            answer = f"Showing OCR not-triggered records for this account ({len(table_rows)} record(s))."
        elif ocr_record_scope == "triggered":
            answer = f"Showing OCR triggered records for this account ({len(table_rows)} record(s))."
        else:
            answer = f"Showing OCR records for this account ({len(table_rows)} record(s))."

        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=ocr_records_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            ocr_records_sql,
            f"ok:ocr_triggered_records:{ocr_record_scope}",
            len(ocr_records_rows),
            answer,
        )
        mark_step("chatbot_ask_ocr_triggered_records_success_return", rows=len(ocr_records_rows), scope=ocr_record_scope)
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_ocr_triggered_breakdown_request(user_query):
        ocr_triggered_sql = build_ocr_triggered_breakdown_sql(account_id)
        ocr_triggered_exec_started_at = time.perf_counter()
        ocr_triggered_rows, ocr_triggered_status = execute_read_only_sql_for_chatbot(ocr_triggered_sql, max_rows=10)
        mark_step(
            "chatbot_ask_ocr_triggered_breakdown_sql_executed",
            rows=len(ocr_triggered_rows),
            sql_status=ocr_triggered_status,
            duration_s=f"{(time.perf_counter() - ocr_triggered_exec_started_at):.3f}",
        )

        if ocr_triggered_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                ocr_triggered_sql,
                f"ocr_triggered_breakdown_sql_failed:{ocr_triggered_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_ocr_triggered_breakdown_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not ocr_triggered_rows:
            answer = "No OCR-trigger data was found for this account."
            log_chat_interaction(
                account_id,
                user_query,
                ocr_triggered_sql,
                "ocr_triggered_breakdown_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_ocr_triggered_breakdown_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(ocr_triggered_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        answer = build_ocr_triggered_breakdown_answer(ocr_triggered_rows)
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=ocr_triggered_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            ocr_triggered_sql,
            "ok:ocr_triggered_breakdown",
            len(ocr_triggered_rows),
            answer,
        )
        mark_step("chatbot_ask_ocr_triggered_breakdown_success_return", rows=len(ocr_triggered_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_ocr_success_breakdown_request(user_query):
        ocr_sql = build_ocr_success_breakdown_sql(account_id)
        ocr_exec_started_at = time.perf_counter()
        ocr_rows, ocr_status = execute_read_only_sql_for_chatbot(ocr_sql, max_rows=10)
        mark_step(
            "chatbot_ask_ocr_success_breakdown_sql_executed",
            rows=len(ocr_rows),
            sql_status=ocr_status,
            duration_s=f"{(time.perf_counter() - ocr_exec_started_at):.3f}",
        )

        if ocr_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                ocr_sql,
                f"ocr_success_breakdown_sql_failed:{ocr_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_ocr_success_breakdown_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not ocr_rows:
            answer = "No OCR records were found for this account."
            log_chat_interaction(
                account_id,
                user_query,
                ocr_sql,
                "ocr_success_breakdown_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_ocr_success_breakdown_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(ocr_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        answer = build_ocr_success_breakdown_answer(ocr_rows)
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=ocr_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            ocr_sql,
            "ok:ocr_success_breakdown",
            len(ocr_rows),
            answer,
        )
        mark_step("chatbot_ask_ocr_success_breakdown_success_return", rows=len(ocr_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if not USE_DYNAMIC_SELECTOR_ONLY and is_account_billing_request(user_query):
        billing_requested_fields = infer_requested_fields_from_query(user_query)
        billing_limit = extract_requested_row_limit(user_query, max_limit=100) or 20
        billing_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        billing_query_limit = None if billing_visual_full_data else billing_limit
        billing_sql = build_account_billing_sql(account_id, user_query, row_limit=billing_query_limit)
        billing_exec_started_at = time.perf_counter()
        billing_rows, billing_status = execute_read_only_sql_for_chatbot(billing_sql, max_rows=billing_query_limit)
        mark_step(
            "chatbot_ask_account_billing_sql_executed",
            rows=len(billing_rows),
            sql_status=billing_status,
            duration_s=f"{(time.perf_counter() - billing_exec_started_at):.3f}",
        )

        if billing_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                billing_sql,
                f"account_billing_sql_failed:{billing_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_account_billing_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not billing_rows and "cc_name" in billing_sql.lower():
            fallback_sql = build_account_billing_sql(account_id, "cc number", row_limit=billing_query_limit)
            fallback_rows, fallback_status = execute_read_only_sql_for_chatbot(fallback_sql, max_rows=billing_query_limit)
            if fallback_status == "ok" and fallback_rows:
                billing_sql = fallback_sql
                billing_rows = fallback_rows
                billing_status = fallback_status

        if not billing_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                billing_sql,
                "account_billing_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_account_billing_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        response_source_rows = billing_rows
        if strict_detail_mode:
            response_source_rows = project_rows_to_requested_fields(billing_rows, billing_requested_fields)
        table_rows = build_table_dataset(response_source_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        answer = build_python_data_answer(user_query, response_source_rows, billing_sql)
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=response_source_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            billing_sql,
            "ok:account_billing",
            len(response_source_rows),
            answer,
        )
        mark_step("chatbot_ask_account_billing_success_return", rows=len(response_source_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if not USE_DYNAMIC_SELECTOR_ONLY and is_latest_packages_request(user_query):
        latest_time_window = extract_requested_relative_time_window(user_query)
        latest_limit = extract_requested_row_limit(user_query, max_limit=100)
        if latest_limit is None and latest_time_window is None:
            latest_limit = max(1, min(int(os.getenv("CHATBOT_LATEST_PACKAGES_DEFAULT_LIMIT", "10")), 100))
        latest_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        latest_query_limit = None if latest_visual_full_data else latest_limit

        latest_sql = build_latest_packages_sql(
            account_id,
            row_limit=latest_query_limit,
            schema_columns=TRACK_PACKAGES_COLUMNS,
            relative_time_window=latest_time_window,
        )
        latest_exec_started_at = time.perf_counter()
        latest_rows, latest_status = execute_read_only_sql_for_chatbot(latest_sql, max_rows=latest_query_limit)
        mark_step(
            "chatbot_ask_latest_packages_sql_executed",
            rows=len(latest_rows),
            sql_status=latest_status,
            duration_s=f"{(time.perf_counter() - latest_exec_started_at):.3f}",
        )

        if latest_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                latest_sql,
                f"latest_packages_sql_failed:{latest_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_latest_packages_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not latest_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                latest_sql,
                "latest_packages_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_latest_packages_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(latest_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query
        if latest_time_window:
            answer = (
                "Here are the package records fetched for "
                f"{latest_time_window.get('phrase', 'the requested time range')} "
                f"({len(table_rows)} record(s))."
            )
        else:
            answer = f"Here are the latest package records fetched for your request ({len(table_rows)} record(s))."
        wants_table_view = query_requests_table_view(user_query, requested_fields=["date_received", "package_id"])
        default_display = "table" if wants_table_view else "key_value"
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            default_display,
            chart_source_rows=latest_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            latest_sql,
            "ok:latest_packages",
            len(latest_rows),
            answer,
        )
        mark_step("chatbot_ask_latest_packages_success_return", rows=len(latest_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    recipient_accuracy_mode = should_use_recipient_accuracy_path(user_query)

    duplicate_name_field = detect_duplicate_name_request_field(user_query)
    if duplicate_name_field != "none":
        duplicate_limit = extract_requested_row_limit(user_query, max_limit=200) or 100
        duplicate_all_columns_mode = should_return_duplicate_name_all_columns(user_query)
        if duplicate_all_columns_mode:
            duplicate_sql = build_duplicate_name_details_sql(account_id, duplicate_name_field, row_limit=duplicate_limit)
        else:
            duplicate_sql = build_duplicate_name_sql(account_id, duplicate_name_field, row_limit=duplicate_limit)
        duplicate_exec_started_at = time.perf_counter()
        duplicate_rows, duplicate_status = execute_read_only_sql_for_chatbot(duplicate_sql, max_rows=duplicate_limit)
        mark_step(
            "chatbot_ask_duplicate_name_sql_executed",
            rows=len(duplicate_rows),
            sql_status=duplicate_status,
            name_field=duplicate_name_field,
            all_columns=duplicate_all_columns_mode,
            duration_s=f"{(time.perf_counter() - duplicate_exec_started_at):.3f}",
        )

        if duplicate_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                duplicate_sql,
                f"duplicate_name_sql_failed:{duplicate_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_duplicate_name_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not duplicate_rows:
            if duplicate_name_field == "last_name":
                answer = "No duplicate last names were found for this account."
            elif duplicate_name_field == "both":
                answer = "No duplicate first names or last names were found for this account."
            else:
                answer = "No duplicate first names were found for this account."
            log_chat_interaction(
                account_id,
                user_query,
                duplicate_sql,
                f"duplicate_name_no_rows:{duplicate_name_field}",
                0,
                answer,
            )
            mark_step("chatbot_ask_duplicate_name_no_rows_return", name_field=duplicate_name_field)
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(duplicate_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query
        session["last_duplicate_name_context"] = {
            "name_field": duplicate_name_field,
            "all_columns": bool(duplicate_all_columns_mode),
            "source_query": user_query,
        }

        if duplicate_all_columns_mode:
            if duplicate_name_field == "last_name":
                answer = f"Showing full recipient rows for duplicate last names ({len(table_rows)} record(s))."
            elif duplicate_name_field == "both":
                answer = f"Showing full recipient rows where first name or last name is duplicated ({len(table_rows)} record(s))."
            else:
                answer = f"Showing full recipient rows for duplicate first names ({len(table_rows)} record(s))."
        else:
            if duplicate_name_field == "last_name":
                answer = f"Found {len(table_rows)} duplicate last name value(s) for this account."
            elif duplicate_name_field == "both":
                answer = f"Found {len(table_rows)} duplicate name value(s) across first and last names for this account."
            else:
                answer = f"Found {len(table_rows)} duplicate first name value(s) for this account."
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=duplicate_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            duplicate_sql,
            f"ok:duplicate_name:{duplicate_name_field}:{'all_columns' if duplicate_all_columns_mode else 'summary'}",
            len(duplicate_rows),
            answer,
        )
        mark_step(
            "chatbot_ask_duplicate_name_success_return",
            rows=len(duplicate_rows),
            name_field=duplicate_name_field,
        )
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    duplicate_context = session.get("last_duplicate_name_context")
    if is_duplicate_name_contextual_followup_request(user_query, duplicate_context):
        followup_duplicate_field = str((duplicate_context or {}).get("name_field", "both") or "both").strip().lower()
        followup_contact_fields = detect_duplicate_name_followup_contact_fields(user_query)
        followup_limit = extract_requested_row_limit(user_query, max_limit=200) or 100
        followup_sql = build_duplicate_name_followup_contact_sql(
            account_id,
            followup_duplicate_field,
            followup_contact_fields,
            row_limit=followup_limit,
        )
        followup_exec_started_at = time.perf_counter()
        followup_rows, followup_status = execute_read_only_sql_for_chatbot(followup_sql, max_rows=followup_limit)
        mark_step(
            "chatbot_ask_duplicate_name_followup_contact_sql_executed",
            rows=len(followup_rows),
            sql_status=followup_status,
            name_field=followup_duplicate_field,
            contact_fields=",".join(followup_contact_fields or ["email"]),
            duration_s=f"{(time.perf_counter() - followup_exec_started_at):.3f}",
        )

        if followup_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                followup_sql,
                f"duplicate_name_followup_contact_sql_failed:{followup_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_duplicate_name_followup_contact_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not followup_rows:
            requested_contact_label = " and ".join(
                "email" if field_name == "email" else "phone"
                for field_name in (followup_contact_fields or ["email"])
            )
            answer = (
                f"No {requested_contact_label} records were found for recipients matched by duplicate-name criteria in this account."
            )
            log_chat_interaction(
                account_id,
                user_query,
                followup_sql,
                "duplicate_name_followup_contact_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_duplicate_name_followup_contact_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(followup_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        requested_contact_label = " and ".join(
            "email" if field_name == "email" else "phone"
            for field_name in (followup_contact_fields or ["email"])
        )
        answer = f"Showing {requested_contact_label} list for duplicate-name recipients ({len(table_rows)} record(s))."
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=followup_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            followup_sql,
            f"ok:duplicate_name_followup_contact:{followup_duplicate_field}",
            len(followup_rows),
            answer,
        )
        mark_step("chatbot_ask_duplicate_name_followup_contact_success_return", rows=len(followup_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if recipient_accuracy_mode and is_top_recipient_request(user_query):
        requested_fields = infer_requested_fields_from_query(user_query)
        requested_top_limit = extract_requested_row_limit(
            user_query,
            max_limit=100,
        ) or extract_ranked_limit_from_raw_query(user_query, max_limit=100)
        top_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        if top_visual_full_data and requested_top_limit is None:
            top_query_limit = None
        else:
            top_query_limit = requested_top_limit or TOP_RECIPIENT_DEFAULT_LIMIT
        top_sql = build_recipient_count_sql(account_id, user_query, row_limit=top_query_limit)
        top_exec_started_at = time.perf_counter()
        top_rows, top_status = execute_read_only_sql_for_chatbot(top_sql, max_rows=top_query_limit)
        mark_step(
            "chatbot_ask_top_recipients_sql_executed",
            rows=len(top_rows),
            sql_status=top_status,
            duration_s=f"{(time.perf_counter() - top_exec_started_at):.3f}",
        )

        if top_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                top_sql,
                f"top_recipients_sql_failed:{top_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_top_recipients_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not top_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                top_sql,
                "top_recipients_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_top_recipients_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        response_source_rows = top_rows
        if strict_detail_mode:
            response_source_rows = project_rows_to_requested_fields(top_rows, requested_fields)
        table_rows = build_table_dataset(response_source_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        ranking_label = "lowest" if is_low_package_count_request(user_query) else "highest"
        answer = f"Here are the recipient package counts ({ranking_label} first, {len(table_rows)} record(s))."
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=response_source_rows,
        )
        if isinstance(chart_payload, dict):
            attach_dashboard_filter_context(
                chart_payload,
                "top_recipients",
                user_query,
                row_limit=top_query_limit,
            )
        log_chat_interaction(
            account_id,
            user_query,
            top_sql,
            "ok:top_recipients",
            len(response_source_rows),
            answer,
        )
        mark_step("chatbot_ask_top_recipients_success_return", rows=len(response_source_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_monthly_delivered_count_request(user_query):
        monthly_sql = build_monthly_delivered_count_sql(account_id, user_query)
        monthly_exec_started_at = time.perf_counter()
        monthly_rows, monthly_status = execute_read_only_sql_for_chatbot(monthly_sql, max_rows=120)
        mark_step(
            "chatbot_ask_monthly_delivered_count_sql_executed",
            rows=len(monthly_rows),
            sql_status=monthly_status,
            duration_s=f"{(time.perf_counter() - monthly_exec_started_at):.3f}",
        )

        if monthly_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                monthly_sql,
                f"monthly_delivered_count_sql_failed:{monthly_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_monthly_delivered_count_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not monthly_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                monthly_sql,
                "monthly_delivered_count_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_monthly_delivered_count_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(monthly_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        monthly_chart_rows = []
        for row in monthly_rows:
            if not isinstance(row, dict):
                continue
            month_value = row.get("delivery_month")
            package_value = _try_parse_float(row.get("package_count"))
            if month_value in (None, "") or package_value is None:
                continue
            month_label = str(month_value).strip()
            if not month_label:
                continue
            if float(package_value).is_integer():
                package_value = int(package_value)
            monthly_chart_rows.append({"delivery_month": month_label, "package_count": package_value})

        answer = build_monthly_delivered_count_answer(monthly_chart_rows or monthly_rows)
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=monthly_chart_rows or monthly_rows,
        )
        if isinstance(chart_payload, dict):
            attach_dashboard_filter_context(
                chart_payload,
                "monthly_delivered_count",
                user_query,
            )
        answer = harmonize_answer_with_display_mode(
            user_query,
            answer,
            response_rows,
            display_mode,
            chart_payload,
        )
        log_chat_interaction(
            account_id,
            user_query,
            monthly_sql,
            "ok:monthly_delivered_count",
            len(monthly_rows),
            answer,
        )
        mark_step("chatbot_ask_monthly_delivered_count_success_return", rows=len(monthly_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_top_carrier_request(user_query):
        requested_fields = infer_requested_fields_from_query(user_query)
        requested_carrier_limit = extract_requested_row_limit(
            user_query,
            max_limit=100,
        ) or extract_ranked_limit_from_raw_query(user_query, max_limit=100)
        carrier_limit = requested_carrier_limit or TOP_RECIPIENT_DEFAULT_LIMIT
        carrier_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        carrier_query_limit = None if (carrier_visual_full_data and requested_carrier_limit is None) else carrier_limit
        carrier_sql = build_carrier_count_sql(account_id, user_query, row_limit=carrier_query_limit)
        carrier_exec_started_at = time.perf_counter()
        carrier_rows, carrier_status = execute_read_only_sql_for_chatbot(carrier_sql, max_rows=carrier_query_limit)
        mark_step(
            "chatbot_ask_top_carriers_sql_executed",
            rows=len(carrier_rows),
            sql_status=carrier_status,
            duration_s=f"{(time.perf_counter() - carrier_exec_started_at):.3f}",
        )

        if carrier_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                carrier_sql,
                f"top_carriers_sql_failed:{carrier_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_top_carriers_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not carrier_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                carrier_sql,
                "top_carriers_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_top_carriers_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        response_source_rows = carrier_rows
        if strict_detail_mode:
            response_source_rows = project_rows_to_requested_fields(carrier_rows, requested_fields)
        table_rows = build_table_dataset(response_source_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        ranking_label = "lowest" if is_low_package_count_request(user_query) else "highest"
        answer = f"Here are the carrier package counts ({ranking_label} first, {len(table_rows)} record(s))."
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=response_source_rows,
        )
        if isinstance(chart_payload, dict):
            attach_dashboard_filter_context(
                chart_payload,
                "top_carriers",
                user_query,
                row_limit=carrier_query_limit,
            )
        log_chat_interaction(
            account_id,
            user_query,
            carrier_sql,
            "ok:top_carriers",
            len(response_source_rows),
            answer,
        )
        mark_step("chatbot_ask_top_carriers_success_return", rows=len(response_source_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_carrier_wise_count_request(user_query):
        requested_fields = infer_requested_fields_from_query(user_query)
        requested_carrier_limit = extract_requested_row_limit(
            user_query,
            max_limit=100,
        ) or extract_ranked_limit_from_raw_query(user_query, max_limit=100)
        carrier_limit = requested_carrier_limit or 100
        carrier_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        carrier_query_limit = None if (carrier_visual_full_data and requested_carrier_limit is None) else carrier_limit
        carrier_sql = build_carrier_count_sql(account_id, user_query, row_limit=carrier_query_limit)
        carrier_exec_started_at = time.perf_counter()
        carrier_rows, carrier_status = execute_read_only_sql_for_chatbot(carrier_sql, max_rows=carrier_query_limit)
        mark_step(
            "chatbot_ask_carrier_wise_count_sql_executed",
            rows=len(carrier_rows),
            sql_status=carrier_status,
            duration_s=f"{(time.perf_counter() - carrier_exec_started_at):.3f}",
        )

        if carrier_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                carrier_sql,
                f"carrier_wise_count_sql_failed:{carrier_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_carrier_wise_count_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not carrier_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                carrier_sql,
                "carrier_wise_count_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_carrier_wise_count_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        response_source_rows = carrier_rows
        if strict_detail_mode:
            response_source_rows = project_rows_to_requested_fields(carrier_rows, requested_fields)
        table_rows = build_table_dataset(response_source_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        answer = f"Here are the carrier-wise package counts ({len(table_rows)} record(s))."
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=response_source_rows,
        )
        if isinstance(chart_payload, dict):
            attach_dashboard_filter_context(
                chart_payload,
                "carrier_wise_count",
                user_query,
                row_limit=carrier_query_limit,
            )
        log_chat_interaction(
            account_id,
            user_query,
            carrier_sql,
            "ok:carrier_wise_count",
            len(response_source_rows),
            answer,
        )
        mark_step("chatbot_ask_carrier_wise_count_success_return", rows=len(response_source_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_carrier_percentage_request(user_query):
        requested_fields = infer_requested_fields_from_query(user_query)
        requested_carrier_limit = extract_requested_row_limit(
            user_query,
            max_limit=100,
        ) or extract_ranked_limit_from_raw_query(user_query, max_limit=100)
        carrier_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        carrier_query_limit = None if (carrier_visual_full_data and requested_carrier_limit is None) else requested_carrier_limit

        carrier_percentage_sql = build_carrier_percentage_sql(account_id, user_query, row_limit=carrier_query_limit)
        carrier_pct_exec_started_at = time.perf_counter()
        carrier_percentage_rows, carrier_percentage_status = execute_read_only_sql_for_chatbot(
            carrier_percentage_sql,
            max_rows=carrier_query_limit,
        )
        mark_step(
            "chatbot_ask_carrier_percentage_sql_executed",
            rows=len(carrier_percentage_rows),
            sql_status=carrier_percentage_status,
            duration_s=f"{(time.perf_counter() - carrier_pct_exec_started_at):.3f}",
        )

        if carrier_percentage_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                carrier_percentage_sql,
                f"carrier_percentage_sql_failed:{carrier_percentage_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_carrier_percentage_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not carrier_percentage_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                carrier_percentage_sql,
                "carrier_percentage_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_carrier_percentage_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        response_source_rows = carrier_percentage_rows
        if strict_detail_mode:
            response_source_rows = project_rows_to_requested_fields(carrier_percentage_rows, requested_fields)
        table_rows = build_table_dataset(response_source_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        answer = build_python_data_answer(user_query, response_source_rows, carrier_percentage_sql)
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=response_source_rows,
        )
        if isinstance(chart_payload, dict):
            attach_dashboard_filter_context(
                chart_payload,
                "carrier_percentage",
                user_query,
                row_limit=carrier_query_limit,
            )
        log_chat_interaction(
            account_id,
            user_query,
            carrier_percentage_sql,
            "ok:carrier_percentage",
            len(response_source_rows),
            answer,
        )
        mark_step("chatbot_ask_carrier_percentage_success_return", rows=len(response_source_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if is_yearly_package_count_request(user_query):
        yearly_sql = build_yearly_package_count_sql(account_id, user_query)
        yearly_exec_started_at = time.perf_counter()
        yearly_rows, yearly_status = execute_read_only_sql_for_chatbot(yearly_sql, max_rows=100)
        mark_step(
            "chatbot_ask_yearly_package_count_sql_executed",
            rows=len(yearly_rows),
            sql_status=yearly_status,
            duration_s=f"{(time.perf_counter() - yearly_exec_started_at):.3f}",
        )

        if yearly_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                yearly_sql,
                f"yearly_package_count_sql_failed:{yearly_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_yearly_package_count_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not yearly_rows:
            answer = build_no_data_fallback_from_query(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                yearly_sql,
                "yearly_package_count_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_yearly_package_count_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        table_rows = build_table_dataset(yearly_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        yearly_chart_rows = []
        for row in yearly_rows:
            if not isinstance(row, dict):
                continue
            year_value = row.get("delivery_year")
            package_value = _try_parse_float(row.get("package_count"))
            if package_value is None:
                continue
            # Use string labels for years so chart inference consistently picks category-vs-metric.
            year_label = str(year_value).strip() if year_value is not None else ""
            if not year_label:
                continue
            if float(package_value).is_integer():
                package_value = int(package_value)
            yearly_chart_rows.append({"delivery_year": year_label, "package_count": package_value})

        answer = build_yearly_package_count_answer(yearly_chart_rows or yearly_rows)

        explicit_chart_requested = bool(operation_profile.get("explicit_chart"))

        if explicit_chart_requested:
            chart_payload = build_chart_payload(yearly_chart_rows or yearly_rows, user_query)
            if chart_payload:
                display_mode = "chart"
                response_rows = table_rows
            else:
                display_mode, response_rows, chart_payload = resolve_visual_response(
                    user_query,
                    table_rows,
                    "table",
                    chart_source_rows=yearly_chart_rows or yearly_rows,
                )
        else:
            display_mode = "table"
            response_rows = table_rows
            chart_payload = None
        answer = harmonize_answer_with_display_mode(
            user_query,
            answer,
            response_rows,
            display_mode,
            chart_payload,
        )
        if isinstance(chart_payload, dict):
            attach_dashboard_filter_context(
                chart_payload,
                "yearly_package_count",
                user_query,
            )
        log_chat_interaction(
            account_id,
            user_query,
            yearly_sql,
            "ok:yearly_package_count",
            len(yearly_rows),
            answer,
        )
        mark_step("chatbot_ask_yearly_package_count_success_return", rows=len(yearly_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if recipient_accuracy_mode and is_recipient_wise_count_request(user_query):
        requested_fields = infer_requested_fields_from_query(user_query)
        recipient_limit = extract_requested_row_limit(user_query, max_limit=100) or 100
        recipient_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        recipient_query_limit = None if recipient_visual_full_data else recipient_limit
        recipient_sql = build_recipient_wise_count_sql(account_id, user_query, row_limit=recipient_query_limit)
        recipient_exec_started_at = time.perf_counter()
        recipient_rows, recipient_status = execute_read_only_sql_for_chatbot(recipient_sql, max_rows=recipient_query_limit)
        mark_step(
            "chatbot_ask_recipient_wise_count_sql_executed",
            rows=len(recipient_rows),
            sql_status=recipient_status,
            duration_s=f"{(time.perf_counter() - recipient_exec_started_at):.3f}",
        )

        if recipient_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                recipient_sql,
                f"recipient_wise_count_sql_failed:{recipient_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_recipient_wise_count_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not recipient_rows:
            answer = build_recipient_template_no_data_answer(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                recipient_sql,
                "recipient_wise_count_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_recipient_wise_count_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        response_source_rows = recipient_rows
        if strict_detail_mode:
            response_source_rows = project_rows_to_requested_fields(recipient_rows, requested_fields)
        table_rows = build_table_dataset(response_source_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query

        answer = build_python_data_answer(user_query, response_source_rows, recipient_sql)
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=response_source_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            recipient_sql,
            "ok:recipient_wise_count",
            len(response_source_rows),
            answer,
        )
        mark_step("chatbot_ask_recipient_wise_count_success_return", rows=len(response_source_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    if not USE_DYNAMIC_SELECTOR_ONLY and delivery_date_intent.get("mode") == "peak":
        mark_step("chatbot_ask_peak_delivered_date_detected")
        peak_sql = build_peak_delivered_date_sql(account_id)

        peak_sql_exec_started_at = time.perf_counter()
        peak_rows, peak_status = execute_read_only_sql_for_chatbot(peak_sql, max_rows=1)
        mark_step(
            "chatbot_ask_peak_delivered_date_sql_executed",
            rows=len(peak_rows),
            sql_status=peak_status,
            duration_s=f"{(time.perf_counter() - peak_sql_exec_started_at):.3f}",
        )

        if peak_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                peak_sql,
                f"peak_delivered_date_sql_failed:{peak_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_peak_delivered_date_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        if not peak_rows:
            answer = "No delivered package records were found for this account."
            log_chat_interaction(
                account_id,
                user_query,
                peak_sql,
                "peak_delivered_date_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_peak_delivered_date_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        top_row = peak_rows[0]
        top_date = format_date_to_mmddyyyy(extract_delivered_date_value(top_row))
        if not top_date:
            top_date = "an available delivery date"
        delivered_count = top_row.get("delivered_count", 0)
        answer = f"Packages were delivered the most on {top_date} with {delivered_count} package(s)."

        session["last_result_rows"] = to_json_safe_rows(peak_rows)
        session["last_user_query"] = user_query
        mark_step("chatbot_ask_peak_delivered_date_context_saved", rows=len(peak_rows))

        log_chat_interaction(
            account_id,
            user_query,
            peak_sql,
            "ok:peak_delivered_date",
            len(peak_rows),
            answer,
        )
        mark_step("chatbot_ask_peak_delivered_date_success_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    if not USE_DYNAMIC_SELECTOR_ONLY and delivery_date_intent.get("mode") in ("latest_date", "dates"):
        mark_step("chatbot_ask_delivered_dates_detected")
        is_latest_date_request = delivery_date_intent.get("mode") == "latest_date"
        delivered_dates_sql = build_delivered_dates_sql(
            account_id,
            limit=1 if is_latest_date_request else 20,
        )

        delivered_dates_exec_started_at = time.perf_counter()
        delivered_date_rows, delivered_date_status = execute_read_only_sql_for_chatbot(
            delivered_dates_sql,
            max_rows=1 if is_latest_date_request else 20,
        )
        mark_step(
            "chatbot_ask_delivered_dates_sql_executed",
            rows=len(delivered_date_rows),
            sql_status=delivered_date_status,
            duration_s=f"{(time.perf_counter() - delivered_dates_exec_started_at):.3f}",
        )

        if delivered_date_status != "ok":
            answer = OUT_OF_DB_RESPONSE
            log_chat_interaction(
                account_id,
                user_query,
                delivered_dates_sql,
                f"delivered_dates_sql_failed:{delivered_date_status}",
                0,
                answer,
            )
            mark_step("chatbot_ask_delivered_dates_failed_return")
            return jsonify({"answer": answer, "rows": [], "status": "sql_execution_failed"})

        dates = []
        for row in delivered_date_rows:
            formatted = format_date_to_mmddyyyy(extract_delivered_date_value(row))
            if formatted and formatted not in dates:
                dates.append(formatted)

        if not dates:
            answer = "No delivered package records were found for this account."
            log_chat_interaction(
                account_id,
                user_query,
                delivered_dates_sql,
                "delivered_dates_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_delivered_dates_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        if is_latest_date_request and dates:
            answer = f"The last delivered package date is **{dates[0]}**."
        elif len(dates) == 1:
            answer = f"Packages were delivered on **{dates[0]}**."
        else:
            answer = f"Packages were delivered on these dates: **{', '.join(dates)}**."

        session["last_result_rows"] = to_json_safe_rows(delivered_date_rows)
        session["last_user_query"] = user_query
        mark_step("chatbot_ask_delivered_dates_context_saved", rows=len(delivered_date_rows))

        log_chat_interaction(
            account_id,
            user_query,
            delivered_dates_sql,
            "ok:delivered_dates",
            len(delivered_date_rows),
            answer,
        )
        mark_step("chatbot_ask_delivered_dates_success_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    recipient_template_intent = detect_recipient_template_intent(user_query) if recipient_accuracy_mode else "none"
    if recipient_template_intent != "none":
        recipient_limit = extract_requested_row_limit(user_query, max_limit=100) or 100
        recipient_visual_full_data = should_fetch_full_data_for_chart_query(user_query)
        recipient_query_limit = None if recipient_visual_full_data else recipient_limit
        if recipient_template_intent == "recipient_count":
            recipient_sql = build_recipient_count_only_sql(account_id, user_query)
        elif recipient_template_intent == "recipient_directory":
            recipient_sql = build_recipient_directory_sql(account_id, user_query, row_limit=recipient_query_limit)
        else:
            recipient_sql = build_recipient_packages_join_sql(account_id, user_query, row_limit=recipient_query_limit)

        recipient_exec_started_at = time.perf_counter()
        recipient_rows, recipient_status = execute_read_only_sql_for_chatbot(recipient_sql, max_rows=recipient_query_limit)
        mark_step(
            "chatbot_ask_recipient_template_sql_executed",
            rows=len(recipient_rows),
            sql_status=recipient_status,
            template=recipient_template_intent,
            duration_s=f"{(time.perf_counter() - recipient_exec_started_at):.3f}",
        )

        if recipient_status != "ok":
            log_chat_interaction(
                account_id,
                user_query,
                recipient_sql,
                f"recipient_template_sql_failed:{recipient_status}",
                0,
                OUT_OF_DB_RESPONSE,
            )
            mark_step("chatbot_ask_recipient_template_failed_return")
            return jsonify({"answer": OUT_OF_DB_RESPONSE, "rows": [], "status": "sql_execution_failed"})

        if not recipient_rows:
            answer = build_recipient_template_no_data_answer(user_query)
            log_chat_interaction(
                account_id,
                user_query,
                recipient_sql,
                "recipient_template_no_rows",
                0,
                answer,
            )
            mark_step("chatbot_ask_recipient_template_no_rows_return")
            return jsonify({"answer": answer, "rows": [], "status": "ok"})

        requested_fields = infer_requested_fields_from_query(user_query)
        response_source_rows = recipient_rows
        if strict_detail_mode:
            response_source_rows = project_rows_to_requested_fields(recipient_rows, requested_fields)

        table_rows = build_table_dataset(response_source_rows, limit=TABLE_VIEW_SAFE_LIMIT)
        session["last_result_rows"] = to_json_safe_rows(table_rows)
        session["last_user_query"] = user_query
        answer = build_python_data_answer(user_query, response_source_rows, recipient_sql)
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            "table",
            chart_source_rows=response_source_rows,
        )
        log_chat_interaction(
            account_id,
            user_query,
            recipient_sql,
            f"ok:{recipient_template_intent}",
            len(response_source_rows),
            answer,
        )
        mark_step("chatbot_ask_recipient_template_success_return", rows=len(response_source_rows))
        update_chart_context(display_mode, chart_payload)
        return jsonify(
            {
                "answer": answer,
                "rows": response_rows,
                "display": display_mode,
                "chart": chart_payload,
                "status": "ok",
            }
        )

    def row_account_id(row):
        if not isinstance(row, dict):
            return ""
        for key in ("account_id", "ACCOUNT_ID", "Account_ID"):
            if key in row and row[key] is not None:
                return str(row[key]).strip()
        return ""

    rewrite_started_at = time.perf_counter()
    skip_query_rewrite = (
        _is_simple_table_direct_request(user_query)
        or _is_table_name_style_request(user_query, SUPPORTED_QUERY_TABLES)
    )
    rewritten_query = user_query if skip_query_rewrite else rewrite_user_query_for_sql(user_query)
    mark_step(
        "chatbot_ask_query_rewritten",
        changed=rewritten_query != user_query,
        skipped=skip_query_rewrite,
        duration_s=f"{(time.perf_counter() - rewrite_started_at):.3f}",
    )
    if ROUTING_DEBUG and rewritten_query != user_query:
        log_routing_debug(
            account_id,
            user_query,
            intent,
            QWEN_MODEL,
            "query_rewrite",
            extra=f"rewritten={rewritten_query}",
        )

    schema_started_at = time.perf_counter()
    schema_map = fetch_schema_metadata_for_chatbot()
    mark_step(
        "chatbot_ask_schema_fetched",
        tables=len(schema_map),
        duration_s=f"{(time.perf_counter() - schema_started_at):.3f}",
    )
    refresh_dynamic_language_resources(schema_map)
    direct_candidate_mode = TABLE_DIRECT_FAST_PATH_ENABLED and _is_simple_table_direct_request(user_query)
    if direct_candidate_mode:
        prompt_tables = tuple()
        prompt_table_source = "direct_fast_precheck"
        explicit_table_mode = False
    else:
        prompt_tables, prompt_table_source, explicit_table_mode = select_prompt_tables_for_query(
            user_query,
            schema_map,
            max_tables=3,
        )
        if _is_ambiguous_marker(prompt_table_source) and len(prompt_tables) > 1:
            answer = build_table_disambiguation_response(prompt_tables)
            log_chat_interaction(
                account_id,
                user_query,
                "",
                f"insufficient_intent:{prompt_table_source}",
                0,
                answer,
            )
            mark_step("chatbot_ask_table_ambiguous")
            return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})

        force_direct_from_exact_prompt = (
            len(prompt_tables) == 1
            and prompt_table_source in EXACT_TABLE_PROMPT_SOURCES
        )
        force_direct_from_table_name_style = (
            len(prompt_tables) == 1
            and _is_table_name_style_request(user_query, _get_runtime_supported_tables(schema_map))
        )
        if force_direct_from_exact_prompt or force_direct_from_table_name_style:
            direct_candidate_mode = True
    direct_table_name = ""
    direct_table_reason = "none"
    generated_sql = ""
    if direct_candidate_mode:
        force_direct_from_exact_prompt = (
            len(prompt_tables) == 1
            and prompt_table_source in EXACT_TABLE_PROMPT_SOURCES
        )
        force_direct_from_table_name_style = (
            len(prompt_tables) == 1
            and _is_table_name_style_request(user_query, _get_runtime_supported_tables(schema_map))
        )
        if force_direct_from_exact_prompt or force_direct_from_table_name_style:
            direct_table_name = prompt_tables[0]
            direct_table_reason = f"prompt_source:{prompt_table_source}"
        else:
            direct_table_name, direct_table_reason = detect_direct_target_table(
                user_query,
                schema_map,
                prompt_tables=prompt_tables,
                prompt_table_source=prompt_table_source,
            )
        if not direct_table_name and _is_ambiguous_marker(direct_table_reason):
            ambiguity_candidates = prompt_tables if prompt_tables else detect_best_query_tables(
                user_query,
                schema_map,
                max_tables=5,
            )
            answer = build_table_disambiguation_response(ambiguity_candidates)
            log_chat_interaction(
                account_id,
                user_query,
                "",
                f"insufficient_intent:{direct_table_reason}",
                0,
                answer,
            )
            mark_step("chatbot_ask_direct_table_ambiguous")
            return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})

        if direct_table_name:
            direct_row_limit = extract_requested_row_limit(user_query, max_limit=200)
            if direct_row_limit is None and should_fetch_full_data_for_chart_query(user_query):
                direct_row_limit = None
            elif direct_row_limit is None:
                direct_row_limit = 100
            generated_sql = build_direct_table_fast_sql(
                account_id,
                user_query,
                direct_table_name,
                schema_map,
                row_limit=direct_row_limit,
            )
            if direct_table_name:
                prompt_tables = (direct_table_name,)
                prompt_table_source = f"direct_resolved:{direct_table_reason}"

    direct_sql_selected = bool(generated_sql)
    schema_text = ""
    allowed_columns_text = ""
    sql_gen_started_at = time.perf_counter()
    if direct_sql_selected:
        sql_gen_timeout = 0
        timeout_policy = f"direct_table_fast_path:{direct_table_reason}"
        sql_cache_hit = False
        generated_sql = normalize_generated_sql_for_log(generated_sql)
        log_routing_debug(
            account_id,
            user_query,
            intent,
            "direct_sql",
            "db_direct_table_fast_path",
            extra=f"table={direct_table_name}; reason={direct_table_reason}; source={prompt_table_source}",
        )
    else:
        schema_text = build_supported_schema_text(schema_map, supported_tables=prompt_tables)
        allowed_columns_text = get_allowed_columns_text(schema_map, supported_tables=prompt_tables)
        sql_gen_timeout, timeout_policy = compute_dynamic_sql_timeout(
            rewritten_query,
            prompt_tables,
            explicit_table_mode=explicit_table_mode,
        )
        generated_sql = _get_cached_sql_generation(account_id, rewritten_query, prompt_tables)
        sql_cache_hit = bool(generated_sql)
        if not sql_cache_hit:
            generated_sql = call_qwen(
                build_sql_prompt(
                    rewritten_query,
                    account_id,
                    schema_text,
                    allowed_columns_text=allowed_columns_text,
                    supported_tables=prompt_tables,
                ),
                timeout_seconds=sql_gen_timeout,
            )
            _set_cached_sql_generation(account_id, rewritten_query, prompt_tables, generated_sql)
    mark_step(
        "chatbot_ask_sql_generated",
        duration_s=f"{(time.perf_counter() - sql_gen_started_at):.3f}",
        timeout_s=sql_gen_timeout,
        timeout_policy=timeout_policy,
        sql_cache_hit=sql_cache_hit,
        prompt_table_source=prompt_table_source,
        explicit_table_mode=explicit_table_mode,
    )
    log_routing_debug(
        account_id,
        user_query,
        intent,
        "direct_sql" if direct_table_name else QWEN_MODEL,
        "db_generate_sql",
        extra=(
            f"source={prompt_table_source}; explicit_table_mode={explicit_table_mode}; "
            f"direct_table={direct_table_name or 'none'}; "
            f"sql_cache_hit={sql_cache_hit}; timeout_s={sql_gen_timeout}; "
            f"timeout_policy={timeout_policy}; tables={','.join(prompt_tables)}"
        ),
    )
    generated_sql = normalize_generated_sql_for_log(generated_sql)

    if generated_sql and (not direct_sql_selected) and not (explicit_table_mode and EXPLICIT_TABLE_SKIP_SECONDARY_LLM):
        mismatch_reason = detect_sql_intent_mismatch_reason(user_query, generated_sql)
        if mismatch_reason:
            alignment_started_at = time.perf_counter()
            aligned_sql = call_qwen(
                build_sql_intent_alignment_prompt(
                    user_query,
                    generated_sql,
                    account_id,
                    schema_text,
                    supported_tables=prompt_tables,
                ),
                timeout_seconds=SECONDARY_QWEN_TIMEOUT,
            )
            mark_step(
                "chatbot_ask_sql_intent_alignment_attempted",
                reason=mismatch_reason,
                duration_s=f"{(time.perf_counter() - alignment_started_at):.3f}",
            )
            aligned_sql = normalize_generated_sql_for_log(aligned_sql)
            if aligned_sql:
                generated_sql = aligned_sql

    requested_fields = []
    skip_requested_fields_inference = operation_profile.get("mode") == "chart" and not strict_detail_mode
    open_data_intent = is_open_table_data_request(user_query)
    req_fields_started_at = time.perf_counter()
    if not skip_requested_fields_inference and not open_data_intent:
        requested_fields = infer_requested_fields_from_query(user_query)
    mark_step(
        "chatbot_ask_requested_fields_inferred",
        fields=len(requested_fields),
        duration_s=f"{(time.perf_counter() - req_fields_started_at):.3f}",
    )
    if (
        generated_sql
        and requested_fields
        and not sql_mentions_requested_fields(generated_sql, requested_fields)
        and (not direct_sql_selected)
        and not (explicit_table_mode and EXPLICIT_TABLE_SKIP_SECONDARY_LLM)
    ):
        repair_started_at = time.perf_counter()
        repaired_sql = call_qwen(
            build_sql_repair_prompt(
                rewritten_query,
                generated_sql,
                requested_fields,
                account_id,
                schema_text,
                supported_tables=prompt_tables,
            ),
            timeout_seconds=SECONDARY_QWEN_TIMEOUT,
        )
        mark_step(
            "chatbot_ask_sql_repair_attempted",
            duration_s=f"{(time.perf_counter() - repair_started_at):.3f}",
        )
        repaired_sql = normalize_generated_sql_for_log(repaired_sql)
        if repaired_sql and sql_mentions_requested_fields(repaired_sql, requested_fields):
            log_routing_debug(
                account_id,
                user_query,
                intent,
                QWEN_MODEL,
                "sql_dynamic_repair",
                extra=f"requested_fields={','.join(requested_fields)}",
            )
            generated_sql = repaired_sql

    if generated_sql:
        is_valid_scope, _ = is_supported_tables_account_scoped(generated_sql, account_id)
        if not is_valid_scope:
            generated_sql = ""

    if generated_sql and open_data_intent and len(prompt_tables) == 1 and not _has_count_intent(user_query):
        requested_limit = extract_requested_row_limit(user_query, max_limit=200)
        generated_sql = build_direct_table_fast_sql(
            account_id,
            user_query,
            prompt_tables[0],
            schema_map,
            row_limit=(requested_limit if requested_limit is not None else 100),
        )
        generated_sql = normalize_generated_sql_for_log(generated_sql)

    chart_full_data_request = bool(operation_profile.get("chart_full_data"))

    if not generated_sql:
        if explicit_table_mode and prompt_tables:
            generated_sql = (
                f"SELECT * FROM {prompt_tables[0]} "
                f"WHERE account_id = {sql_literal(account_id)}"
            )
            if not chart_full_data_request:
                generated_sql += " LIMIT 100"
        else:
            generated_sql, fallback_source = build_backend_fallback_sql(user_query, account_id, schema_map)
            if not generated_sql and str(fallback_source).startswith("fallback_no_"):
                candidate_tables = detect_prompt_table_candidates(user_query, schema_map, max_tables=6)
                if len(candidate_tables) == 1:
                    generated_sql, candidate_status = build_account_scoped_candidate_sql(
                        candidate_tables[0],
                        account_id,
                        schema_map,
                        row_limit=100,
                    )
                    if generated_sql:
                        fallback_source = f"candidate_select:{candidate_tables[0]}"
                    else:
                        fallback_source = f"fallback_no_candidate_sql:{candidate_status}"
                if not generated_sql:
                    answer = (
                        build_table_disambiguation_response(candidate_tables)
                        if len(candidate_tables) > 1
                        else INTENT_CLARIFICATION_RESPONSE
                    )
                    log_chat_interaction(
                        account_id,
                        user_query,
                        "",
                        f"insufficient_intent:{fallback_source}",
                        0,
                        answer,
                    )
                    mark_step("chatbot_ask_insufficient_intent")
                    return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})
        generated_sql = normalize_generated_sql_for_log(generated_sql)

    if chart_full_data_request and generated_sql and not should_preserve_generated_limit_for_chart(user_query):
        generated_sql = _strip_trailing_limit_clause(generated_sql)

    if not generated_sql:
        answer = INTENT_CLARIFICATION_RESPONSE
        log_chat_interaction(
            account_id,
            user_query,
            "",
            "insufficient_intent:no_sql_generated",
            0,
            answer,
        )
        mark_step("chatbot_ask_insufficient_intent_no_sql")
        return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})

    query_table_ok, query_table_reason = validate_query_table_relevance(user_query, generated_sql)
    if not query_table_ok:
        answer = QUERY_REPHRASE_RESPONSE
        log_chat_interaction(
            account_id,
            user_query,
            generated_sql,
            f"query_table_mismatch:{query_table_reason}",
            0,
            answer,
        )
        mark_step("chatbot_ask_query_table_mismatch")
        return jsonify({"answer": answer, "rows": [], "status": "query_table_mismatch"})

    explicit_table_ok, explicit_table_reason, explicit_tables = validate_explicit_table_alignment(
        user_query,
        generated_sql,
        schema_map,
    )
    if not explicit_table_ok:
        answer = QUERY_REPHRASE_RESPONSE
        log_chat_interaction(
            account_id,
            user_query,
            generated_sql,
            f"query_table_mismatch:{explicit_table_reason}",
            0,
            answer,
        )
        mark_step("chatbot_ask_explicit_table_mismatch")
        return jsonify({"answer": answer, "rows": [], "status": "query_table_mismatch"})

    sql_exec_started_at = time.perf_counter()
    sql_exec_limit = None if chart_full_data_request else 100
    sql_rows, sql_status, sql_source = execute_query_with_dataset_fallback(
        generated_sql,
        account_id,
        schema_map,
        max_rows=sql_exec_limit,
    )
    mark_step(
        "chatbot_ask_sql_executed",
        rows=len(sql_rows),
        sql_status=sql_status,
        source=sql_source,
        duration_s=f"{(time.perf_counter() - sql_exec_started_at):.3f}",
    )
    log_routing_debug(
        account_id,
        user_query,
        intent,
        QWEN_MODEL,
        "db_sql_executed",
        extra=f"sql_status={sql_status}; rows={len(sql_rows)}",
    )
    if sql_status != "ok":
        retry_sql, retry_source = build_backend_fallback_sql(user_query, account_id, schema_map)
        if not retry_sql and str(retry_source).startswith("fallback_no_"):
            candidate_tables = detect_prompt_table_candidates(user_query, schema_map, max_tables=6)
            if len(candidate_tables) == 1:
                retry_sql, candidate_status = build_account_scoped_candidate_sql(
                    candidate_tables[0],
                    account_id,
                    schema_map,
                    row_limit=sql_exec_limit if isinstance(sql_exec_limit, int) else 100,
                )
                if retry_sql:
                    retry_source = f"candidate_select:{candidate_tables[0]}"
                else:
                    retry_source = f"fallback_no_candidate_sql:{candidate_status}"
            if not retry_sql:
                answer = (
                    build_table_disambiguation_response(candidate_tables)
                    if len(candidate_tables) > 1
                    else INTENT_CLARIFICATION_RESPONSE
                )
                log_chat_interaction(
                    account_id,
                    user_query,
                    generated_sql,
                    f"insufficient_intent:{retry_source}",
                    0,
                    answer,
                )
                mark_step("chatbot_ask_sql_retry_insufficient_intent")
                return jsonify({"answer": answer, "rows": [], "status": "insufficient_intent"})

        retry_sql = normalize_generated_sql_for_log(retry_sql)
        if chart_full_data_request and retry_sql and not should_preserve_generated_limit_for_chart(user_query):
            retry_sql = _strip_trailing_limit_clause(retry_sql)
        retry_exec_started_at = time.perf_counter()
        retry_rows, retry_status, retry_source_name = execute_query_with_dataset_fallback(
            retry_sql,
            account_id,
            schema_map,
            max_rows=sql_exec_limit,
        )
        mark_step(
            "chatbot_ask_sql_retry_executed",
            rows=len(retry_rows),
            retry_status=retry_status,
            source=f"{retry_source}/{retry_source_name}",
            duration_s=f"{(time.perf_counter() - retry_exec_started_at):.3f}",
        )
        log_routing_debug(
            account_id,
            user_query,
            intent,
            "backend_fallback_sql",
            "db_sql_retry_fallback",
            extra=f"source={retry_source}; sql_status={retry_status}; rows={len(retry_rows)}",
        )

        if retry_status == "ok":
            generated_sql = retry_sql
            sql_rows = retry_rows
            sql_status = retry_status
        else:
            log_chat_interaction(
                account_id,
                user_query,
                generated_sql,
                f"sql_execution_failed:{sql_status}",
                0,
                OUT_OF_DB_RESPONSE,
            )
            mark_step("chatbot_ask_sql_failed_return")
            return jsonify({"answer": OUT_OF_DB_RESPONSE, "rows": [], "status": "sql_execution_failed"})

    # Dynamic retry: when SQL returns no rows, ask the model to repair constraints dynamically.
    if sql_status == "ok" and not sql_rows and (not direct_sql_selected) and not (explicit_table_mode and EXPLICIT_TABLE_SKIP_SECONDARY_LLM):
        relax_started_at = time.perf_counter()
        relaxed_sql = call_qwen(
            build_sql_relax_prompt(
                rewritten_query,
                generated_sql,
                account_id,
                schema_text,
                supported_tables=prompt_tables,
            ),
            timeout_seconds=SECONDARY_QWEN_TIMEOUT,
        )
        mark_step(
            "chatbot_ask_sql_relax_generated",
            duration_s=f"{(time.perf_counter() - relax_started_at):.3f}",
        )
        relaxed_sql = normalize_generated_sql_for_log(relaxed_sql)
        if chart_full_data_request and relaxed_sql and not should_preserve_generated_limit_for_chart(user_query):
            relaxed_sql = _strip_trailing_limit_clause(relaxed_sql)
        if relaxed_sql:
            is_relaxed_valid, _ = is_supported_tables_account_scoped(relaxed_sql, account_id)
            if is_relaxed_valid:
                relax_exec_started_at = time.perf_counter()
                relaxed_rows, relaxed_status, relaxed_source = execute_query_with_dataset_fallback(
                    relaxed_sql,
                    account_id,
                    schema_map,
                    max_rows=sql_exec_limit,
                )
                mark_step(
                    "chatbot_ask_sql_relax_executed",
                    rows=len(relaxed_rows),
                    relax_status=relaxed_status,
                    source=relaxed_source,
                    duration_s=f"{(time.perf_counter() - relax_exec_started_at):.3f}",
                )
                log_routing_debug(
                    account_id,
                    user_query,
                    intent,
                    QWEN_MODEL,
                    "sql_dynamic_relax_retry",
                    extra=f"sql_status={relaxed_status}; rows={len(relaxed_rows)}",
                )
                if relaxed_status == "ok" and relaxed_rows:
                    generated_sql = relaxed_sql
                    sql_rows = relaxed_rows
                    sql_status = relaxed_status

    aggregate_query_mode = is_aggregate_sql_query(generated_sql)
    if aggregate_query_mode:
        matched_rows = []
        mark_step("chatbot_ask_session_filter_skipped", reason="aggregate_sql", sql_rows=len(sql_rows))
    else:
        filtered_data_with_account_id = [
            row for row in session_rows if row_account_id(row) == str(account_id).strip()
        ]
        matched_rows = filter_session_rows_by_query(filtered_data_with_account_id, user_query)
        mark_step("chatbot_ask_session_filter_applied", matched_rows=len(matched_rows), sql_rows=len(sql_rows))
        if matched_rows and sql_rows:
            session_filtered_keys = {_row_identity(row) for row in matched_rows[:1000]}
            filtered_sql_rows = []
            for row in sql_rows:
                row_key = _row_identity(row)
                if row_key in session_filtered_keys:
                    filtered_sql_rows.append(row)
            if filtered_sql_rows:
                sql_rows = filtered_sql_rows
                mark_step("chatbot_ask_sql_rows_intersected", rows=len(sql_rows))

    requested_limit = extract_requested_row_limit(
        user_query,
        max_limit=100,
    ) or extract_ranked_limit_from_raw_query(user_query, max_limit=100)
    enforce_top_n_limit = should_enforce_top_n_limit(user_query)
    if requested_limit and len(sql_rows) > requested_limit:
        if (not chart_full_data_request) or enforce_top_n_limit:
            sql_rows = sql_rows[:requested_limit]
            mark_step(
                "chatbot_ask_user_row_limit_applied",
                row_limit=requested_limit,
                rows=len(sql_rows),
                strict_top_n=enforce_top_n_limit,
            )

    if strict_detail_mode:
        sql_rows = project_rows_to_requested_fields(sql_rows, requested_fields)
        mark_step("chatbot_ask_requested_fields_projection_applied", rows=len(sql_rows), fields=len(requested_fields))

    reshaped_sql_rows = reshape_rows_for_chart(user_query, sql_rows)
    if reshaped_sql_rows is not sql_rows:
        sql_rows = reshaped_sql_rows
        mark_step("chatbot_ask_chart_rows_reshaped", rows=len(sql_rows))

    if not sql_rows:
        if explicit_tables:
            answer = build_explicit_table_no_data_response(explicit_tables)
        else:
            answer = build_no_data_fallback_from_query(user_query)
        log_chat_interaction(
            account_id,
            user_query,
            generated_sql,
            "insufficient_data",
            0,
            answer,
        )
        mark_step("chatbot_ask_no_data_return")
        return jsonify({"answer": answer, "rows": [], "status": "ok"})

    # Store last SQL result set for follow-up references like "what are they?".
    table_rows = build_table_dataset(sql_rows, limit=TABLE_VIEW_SAFE_LIMIT)
    session["last_result_rows"] = to_json_safe_rows(table_rows)
    session["last_user_query"] = user_query
    mark_step("chatbot_ask_session_context_saved", rows=len(table_rows))

    answer = build_python_data_answer(user_query, sql_rows, generated_sql)
    single_column_request = len(requested_fields) == 1 if isinstance(requested_fields, list) else False
    single_column_result = is_single_column_result(sql_rows)
    wants_table_view = query_requests_table_view(user_query, requested_fields=requested_fields)
    runtime_tables = _get_runtime_supported_tables(schema_map)
    table_name_style_request = _is_table_name_style_request(user_query, runtime_tables)
    if wants_table_view or table_name_style_request:
        default_display = "table"
    elif single_column_request or single_column_result:
        default_display = "text"
    else:
        default_display = "key_value"

    explicit_chart_requested = bool(operation_profile.get("mode") == "chart" or operation_profile.get("explicit_chart"))
    if explicit_chart_requested:
        chart_payload = _get_cached_chart_payload(account_id, user_query, generated_sql)
        if chart_payload:
            display_mode = "chart"
            response_rows = table_rows
            answer = f"Generated chart data with {int(chart_payload.get('point_count', 0) or len(sql_rows))} point(s)."
            mark_step("chatbot_ask_chart_payload_cache_hit")
        else:
            chart_payload = build_chart_payload(sql_rows, user_query)
            if chart_payload:
                _set_cached_chart_payload(account_id, user_query, generated_sql, chart_payload)
                display_mode = "chart"
                response_rows = table_rows
                answer = f"Generated chart data with {int(chart_payload.get('point_count', 0) or len(sql_rows))} point(s)."
                mark_step("chatbot_ask_chart_payload_cache_store")
            else:
                display_mode, response_rows, chart_payload = resolve_visual_response(
                    user_query,
                    table_rows,
                    default_display,
                    chart_source_rows=sql_rows,
                )
    else:
        display_mode, response_rows, chart_payload = resolve_visual_response(
            user_query,
            table_rows,
            default_display,
            chart_source_rows=sql_rows,
        )
    answer = harmonize_answer_with_display_mode(
        user_query,
        answer,
        response_rows,
        display_mode,
        chart_payload,
    )

    log_chat_interaction(
        account_id,
        user_query,
        generated_sql,
        "ok:sql_rows_python",
        len(sql_rows),
        answer,
    )
    mark_step("chatbot_ask_success_return", rows=len(sql_rows))
    update_chart_context(display_mode, chart_payload)
    return jsonify(
        {
            "answer": answer,
            "rows": response_rows,
            "display": display_mode,
            "chart": chart_payload,
            "status": "ok",
        }
    )


@app.route("/chatbot/dashboard/filter", methods=["POST"])
def chatbot_dashboard_filter():
    """Recalculate dashboard charts with explicit date range filters."""
    account_id = session.get("account_id")
    if not account_id:
        return jsonify({"error": "Session expired. Please login again."}), 401

    payload = request.get_json(silent=True) or {}
    filter_context = payload.get("filter_context") if isinstance(payload.get("filter_context"), dict) else {}
    filter_kind = str(filter_context.get("kind", "")).strip().lower()
    user_query = str(filter_context.get("user_query", "")).strip()

    if not filter_kind:
        return jsonify({"error": "filter_context.kind is required"}), 400

    start_date = _normalize_iso_date(payload.get("from_date"))
    end_date = _normalize_iso_date(payload.get("to_date"))

    row_limit = filter_context.get("row_limit")
    try:
        if row_limit is not None:
            row_limit = max(1, min(int(row_limit), 200))
    except (TypeError, ValueError):
        row_limit = None

    effective_query = user_query or filter_kind.replace("_", " ")

    sql_text = ""
    max_rows = row_limit
    if filter_kind in ("top_recipients", "recipient_wise_count"):
        sql_text = build_recipient_count_sql(
            account_id,
            effective_query,
            row_limit=row_limit,
            start_date=start_date,
            end_date=end_date,
            override_time_filters=True,
        )
    elif filter_kind in ("carrier_percentage",):
        sql_text = build_carrier_percentage_sql(
            account_id,
            effective_query,
            row_limit=row_limit,
            start_date=start_date,
            end_date=end_date,
            override_time_filters=True,
        )
    elif filter_kind in ("top_carriers", "carrier_wise_count"):
        sql_text = build_carrier_count_sql(
            account_id,
            effective_query,
            row_limit=row_limit,
            start_date=start_date,
            end_date=end_date,
            override_time_filters=True,
        )
    elif filter_kind in ("monthly_delivered_count",):
        sql_text = build_monthly_delivered_count_sql(
            account_id,
            effective_query,
            start_date=start_date,
            end_date=end_date,
            override_time_filters=True,
        )
    elif filter_kind in ("yearly_package_count",):
        sql_text = build_yearly_package_count_sql(
            account_id,
            effective_query,
            start_date=start_date,
            end_date=end_date,
            override_time_filters=True,
        )
    else:
        return jsonify({"status": "unsupported", "error": f"Unsupported filter kind: {filter_kind}"}), 400

    rows, status = execute_read_only_sql_for_chatbot(sql_text, max_rows=max_rows)
    if status != "ok":
        return jsonify({"status": "sql_execution_failed", "error": OUT_OF_DB_RESPONSE}), 500

    if not rows:
        return jsonify(
            {
                "status": "ok",
                "rows": [],
                "chart": None,
                "summary": {"total_packages": 0, "row_count": 0},
            }
        )

    chart_payload = build_chart_payload(rows, effective_query)
    if isinstance(chart_payload, dict):
        attach_dashboard_filter_context(chart_payload, filter_kind, user_query, row_limit=row_limit)

    total_packages = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        package_count = _try_parse_float(row.get("package_count"))
        if package_count is not None:
            total_packages += package_count

    if abs(total_packages - int(total_packages)) < 1e-9:
        total_packages = int(total_packages)
    else:
        total_packages = round(total_packages, 2)

    return jsonify(
        {
            "status": "ok",
            "rows": build_table_dataset(rows, limit=TABLE_VIEW_SAFE_LIMIT),
            "chart": chart_payload,
            "summary": {
                "total_packages": total_packages,
                "row_count": len(rows),
            },
        }
    )


@app.route("/logout", methods=["GET"])
def logout():
    session.clear()
    return redirect(url_for("login_page"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
