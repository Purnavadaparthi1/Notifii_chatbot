import logging

from mcp.server.fastmcp import FastMCP

from mcp_bridge import (
    execute_read_only_sql_tool,
    get_schema_metadata_tool,
    validate_read_only_sql_tool,
)


logger = logging.getLogger("notifii_mcp_server")
mcp = FastMCP("notifii-chatbot-mcp")


@mcp.tool()
def get_schema_metadata(force_refresh=False):
    """Return DB schema metadata map and compact schema text."""
    return get_schema_metadata_tool(force_refresh=force_refresh)


@mcp.tool()
def validate_read_only_sql(sql_text):
    """Validate whether SQL is read-only and safe for execution."""
    return validate_read_only_sql_tool(sql_text)


@mcp.tool()
def execute_read_only_sql(sql_text, max_rows=100):
    """Execute validated read-only SQL and return JSON-safe rows."""
    return execute_read_only_sql_tool(sql_text=sql_text, max_rows=max_rows)


if __name__ == "__main__":
    logger.info("Starting MCP server: notifii-chatbot-mcp")
    mcp.run()
