import logging

from db_engine import (
    execute_read_only_sql,
    fetch_schema_metadata,
    format_schema_for_prompt,
    validate_read_only_sql,
)


logger = logging.getLogger("mcp_bridge")


def get_schema_metadata_tool(force_refresh=False):
    """MCP-compatible schema metadata tool response."""
    schema_map = fetch_schema_metadata(force_refresh=force_refresh)
    return {
        "schema_map": schema_map,
        "schema_text": format_schema_for_prompt(schema_map),
        "table_count": len(schema_map),
    }


def validate_read_only_sql_tool(sql_text):
    """MCP-compatible SQL validation tool response."""
    is_valid, reason = validate_read_only_sql(sql_text)
    return {"is_valid": bool(is_valid), "reason": str(reason)}


def execute_read_only_sql_tool(sql_text, max_rows=100):
    """MCP-compatible SQL execution tool response."""
    is_valid, reason = validate_read_only_sql(sql_text)
    if not is_valid:
        return {
            "rows": [],
            "status": reason,
            "row_count": 0,
            "validated": False,
        }

    rows, status = execute_read_only_sql(sql_text, max_rows=max_rows)
    if status != "ok":
        logger.warning("MCP SQL tool execution failed: %s", status)
    return {
        "rows": rows,
        "status": status,
        "row_count": len(rows),
        "validated": True,
    }
