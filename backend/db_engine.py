import logging
import os
import re
import time
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


# Basic logging setup (console + file).
LOG_PATH = Path(__file__).resolve().parent / "db_connection.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger("db_engine")

# Load environment variables from backend/.env
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)


DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "password")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "3306")
    database = os.getenv("DB_NAME", "testdb")
    DATABASE_URL = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    logger.info("Using DB_* environment settings for SQLAlchemy engine.")

logger.info("Using SQLAlchemy MySQL engine.")
DB_CONNECT_TIMEOUT_SECONDS = max(1, int(os.getenv("DB_CONNECT_TIMEOUT_SECONDS", "5")))
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"connect_timeout": DB_CONNECT_TIMEOUT_SECONDS},
)
CURRENT_ACCOUNT_ID = None
SCHEMA_CACHE_TTL_SECONDS = max(5, int(os.getenv("SCHEMA_CACHE_TTL_SECONDS", "300")))
_SCHEMA_CACHE = {
    "loaded_at": 0.0,
    "db_name": "",
    "schema": {},
}
READ_ONLY_BLOCKLIST = (
    "insert",
    "update",
    "delete",
    "drop",
    "truncate",
    "alter",
    "create",
    "replace",
    "grant",
    "revoke",
    "call",
)


def set_current_account_id(account_id):
    """Set global account_id from successful login response."""
    global CURRENT_ACCOUNT_ID
    CURRENT_ACCOUNT_ID = str(account_id).strip() if account_id is not None else None
    logger.info("Global account_id updated to %s", CURRENT_ACCOUNT_ID)


def fetch_track_packages_data(account_id, max_rows=500, include_status=False):
    """Fetch account-scoped data only from track_packages table."""
    resolved_account_id = str(account_id).strip() if account_id is not None else ""
    if not resolved_account_id:
        logger.warning("No account_id provided for track_packages query.")
        if include_status:
            return [], "missing_account_id"
        return []

    safe_limit = max(1, min(int(max_rows), 2000))
    query = text(
        f"SELECT * FROM track_packages WHERE account_id = :account_id LIMIT {safe_limit}"
    )

    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"account_id": resolved_account_id})
            filtered_dataset = [dict(row._mapping) for row in result.fetchall()]
            logger.info(
                "Fetched %s rows from track_packages(account_id=%s)",
                len(filtered_dataset),
                resolved_account_id,
            )
            if include_status:
                return filtered_dataset, "ok"
            return filtered_dataset
    except SQLAlchemyError as error:
        logger.error(
            "Failed to fetch track_packages(account_id=%s): %s",
            resolved_account_id,
            error,
        )
        if include_status:
            return [], f"sql_error:{error.__class__.__name__}"
        return []


def get_database_name():
    """Resolve the active database name for schema introspection."""
    db_name = os.getenv("DB_NAME")
    if db_name:
        return db_name
    return engine.url.database


def fetch_schema_metadata(force_refresh=False):
    """Fetch table/column metadata from information_schema with TTL cache."""
    db_name = get_database_name()
    if not db_name:
        logger.warning("Cannot fetch schema metadata: database name not resolved.")
        return {}

    cache_is_valid = (
        not force_refresh
        and _SCHEMA_CACHE.get("schema")
        and _SCHEMA_CACHE.get("db_name") == db_name
        and (time.time() - float(_SCHEMA_CACHE.get("loaded_at", 0.0))) < SCHEMA_CACHE_TTL_SECONDS
    )
    if cache_is_valid:
        return _SCHEMA_CACHE["schema"]

    schema_query = text(
        """
        SELECT table_name AS table_name, column_name AS column_name
        FROM information_schema.columns
        WHERE table_schema = :db_name
        ORDER BY table_name, ordinal_position
        """
    )

    schema = {}
    try:
        with engine.connect() as connection:
            result = connection.execute(schema_query, {"db_name": db_name})
            for row in result.fetchall():
                mapping = row._mapping
                table_name = (
                    mapping.get("table_name")
                    or mapping.get("TABLE_NAME")
                    or row[0]
                )
                column_name = (
                    mapping.get("column_name")
                    or mapping.get("COLUMN_NAME")
                    or row[1]
                )
                schema.setdefault(table_name, []).append(column_name)

            _SCHEMA_CACHE["schema"] = schema
            _SCHEMA_CACHE["db_name"] = db_name
            _SCHEMA_CACHE["loaded_at"] = time.time()
        return schema
    except SQLAlchemyError as error:
        logger.error("Failed to fetch schema metadata: %s", error)
        return {}


def format_schema_for_prompt(schema_map):
    """Convert schema map to compact text for prompt context."""
    if not schema_map:
        return "No schema metadata available."
    lines = []
    for table_name, columns in schema_map.items():
        lines.append(f"{table_name}({', '.join(columns)})")
    return "\n".join(lines)


def normalize_sql(sql_text):
    """Strip markdown fences and normalize whitespace for execution."""
    if not sql_text:
        return ""
    cleaned = sql_text.strip()
    cleaned = re.sub(r"^```(?:sql)?", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"```$", "", cleaned).strip()
    cleaned = cleaned.rstrip(";").strip()
    return cleaned


def validate_read_only_sql(sql_text):
    """Allow only SELECT/CTE SQL and block dangerous statements."""
    normalized = normalize_sql(sql_text)
    lowered = normalized.lower()

    if not normalized:
        return False, "Generated SQL is empty."

    if ";" in normalized:
        return False, "Multiple SQL statements are not allowed."

    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "Only SELECT/CTE queries are allowed."

    for blocked in READ_ONLY_BLOCKLIST:
        if re.search(rf"\b{blocked}\b", lowered):
            return False, f"Blocked SQL keyword detected: {blocked}."

    return True, "ok"


def execute_read_only_sql(sql_text, max_rows=100):
    """Execute a validated read-only query and return rows as dictionaries."""
    is_valid, reason = validate_read_only_sql(sql_text)
    if not is_valid:
        logger.warning("Rejected SQL query. reason=%s sql=%s", reason, sql_text)
        return [], reason

    normalized = normalize_sql(sql_text)
    limited_query = normalized
    if max_rows is not None:
        safe_limit = max(1, min(int(max_rows), 200))
        if " limit " not in normalized.lower():
            limited_query = f"{normalized} LIMIT {safe_limit}"

    try:
        with engine.connect() as connection:
            result = connection.execute(text(limited_query))
            filtered_dataset = [dict(row._mapping) for row in result.fetchall()]
            return filtered_dataset, "ok"
    except SQLAlchemyError as error:
        logger.error("Failed to execute generated SQL: %s", error)
        return [], str(error)



def test_connection():
    """Try opening a DB connection and log whether it works."""
    try:
        with engine.connect():
            logger.info("MySQL connection successful.")
            print("Connected Successfully!")
            return True
    except SQLAlchemyError as error:
        logger.error("MySQL connection failed: %s", error)
        return False


if __name__ == "__main__":
    test_connection()


