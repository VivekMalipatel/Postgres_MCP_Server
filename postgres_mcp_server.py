from typing import Any, Dict, List, Optional, Union
import json
import os
import asyncio
import httpx
import traceback
import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

import dotenv
from mcp.server.fastmcp import Context, FastMCP

from postgres_manager import PostgresManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
dotenv.load_dotenv()

@dataclass
class AppContext:
    """Application context holding initialized resources."""
    db: PostgresManager

@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage database lifecycle with type-safe context."""
    # Initialize database connection on startup
    logger.info("Initializing PostgreSQL connection...")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "postgres")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "")
    
    db = PostgresManager(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    
    try:
        await db.connect()
        logger.info("PostgreSQL connection established successfully")
        yield AppContext(db=db)
    finally:
        # Cleanup on shutdown
        logger.info("Closing PostgreSQL connection...")
        await db.close()
        logger.info("PostgreSQL connection closed")

# Initialize FastMCP server with lifespan
mcp = FastMCP("PostgreSQL MCP", 
              description="MCP server for PostgreSQL database operations",
              lifespan=app_lifespan)

# Constants
DEFAULT_LIMIT = 100

# ===== Resources =====

@mcp.resource("postgres://tables")
async def get_tables() -> str:
    """Get a list of tables in the database."""
    logger.info("Resource accessed: get_tables()")
    # Access the database through the global context
    context = mcp.get_request_context()
    db = context.lifespan_context.db
    tables = await db.get_tables()
    logger.info(f"Retrieved {len(tables)} tables from database")
    return json.dumps(tables, indent=2)

@mcp.resource("postgres://schema/{table_name}")
async def get_table_schema(table_name: str) -> str:
    """Get schema information for a specific table.
    
    Args:
        table_name: The name of the table to retrieve schema for
    """
    logger.info(f"Resource accessed: get_table_schema(table_name='{table_name}')")
    context = mcp.get_request_context()
    db = context.lifespan_context.db
    schema = await db.get_table_schema(table_name)
    logger.info(f"Retrieved schema for table '{table_name}'")
    return json.dumps(schema, indent=2)

@mcp.resource("postgres://data/{table_name}")
async def get_all_data(table_name: str) -> str:
    """Get all data from a specific table (limited to 100 rows for safety).
    
    Args:
        table_name: The name of the table to retrieve data from
    """
    logger.info(f"Resource accessed: get_all_data(table_name='{table_name}')")
    context = mcp.get_request_context()
    db = context.lifespan_context.db
    data = await db.select_data(table_name, limit=DEFAULT_LIMIT)
    logger.info(f"Retrieved {len(data)} rows from table '{table_name}'")
    return json.dumps(data, indent=2)

# ===== Tools =====

@mcp.tool(description="Execute a custom SQL query against the PostgreSQL database.")
async def execute_query(ctx: Context, query: str) -> str:
    """Execute a raw SQL query.

    Args:
        query: SQL query to execute
    """
    logger.info(f"Tool called: execute_query(query='{query[:50]}...' if len(query) > 50 else query)")
    db = ctx.request_context.lifespan_context.db
    try:
        result = await db.execute_query(query)
        logger.info("Query executed successfully")
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}", exc_info=True)
        return json.dumps({"error": str(e), "details": traceback.format_exc()})

@mcp.tool(description="Create a new table in the PostgreSQL database with specified columns.")
async def create_table(ctx: Context, table_name: str, columns: List[Dict[str, str]]) -> str:
    """Create a new table in the database.
    
    Args:
        table_name: Name of the table to create
        columns: List of column definitions with name and type
                [{"name": "id", "type": "SERIAL PRIMARY KEY"}, 
                 {"name": "title", "type": "VARCHAR(255) NOT NULL"}]
    """
    logger.info(f"Tool called: create_table(table_name='{table_name}', columns={columns})")
    db = ctx.request_context.lifespan_context.db
    try:
        await db.create_table(table_name, columns)
        logger.info(f"Table '{table_name}' created successfully")
        return f"Table '{table_name}' created successfully"
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {str(e)}", exc_info=True)
        return json.dumps({"error": str(e), "details": traceback.format_exc()})

@mcp.tool(description="Drop (delete) an existing table from the PostgreSQL database.")
async def drop_table(ctx: Context, table_name: str) -> str:
    """Drop a table from the database.
    
    Args:
        table_name: Name of the table to drop
    """
    logger.info(f"Tool called: drop_table(table_name='{table_name}')")
    db = ctx.request_context.lifespan_context.db
    try:
        await db.drop_table(table_name)
        logger.info(f"Table '{table_name}' dropped successfully")
        return f"Table '{table_name}' dropped successfully"
    except Exception as e:
        logger.error(f"Error dropping table '{table_name}': {str(e)}", exc_info=True)
        return json.dumps({"error": str(e), "details": traceback.format_exc()})

@mcp.tool(description="Insert a new row of data into a PostgreSQL table.")
async def insert_data(ctx: Context, table_name: str, data: Dict[str, Any]) -> str:
    """Insert a row into a table.
    
    Args:
        table_name: Target table
        data: Dictionary with column-value pairs
    """
    logger.info(f"Tool called: insert_data(table_name='{table_name}', data={data})")
    db = ctx.request_context.lifespan_context.db
    try:
        result = await db.insert_data(table_name, data)
        logger.info(f"Data inserted successfully into table '{table_name}'")
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        logger.error(f"Error inserting data into table '{table_name}': {str(e)}", exc_info=True)
        return json.dumps({"error": str(e), "details": traceback.format_exc()})

@mcp.tool(description="Update existing rows in a PostgreSQL table that match a condition.")
async def update_data(ctx: Context, table_name: str, data: Dict[str, Any], condition: str, condition_params: List[Any]) -> str:
    """Update rows in a table.
    
    Args:
        table_name: Target table
        data: Dictionary with column-value pairs to update
        condition: WHERE clause (e.g., "id = %s")
        condition_params: Parameters for the condition
    """
    logger.info(f"Tool called: update_data(table_name='{table_name}', data={data}, condition='{condition}', condition_params={condition_params})")
    db = ctx.request_context.lifespan_context.db
    try:
        result = await db.update_data(table_name, data, condition, tuple(condition_params))
        logger.info(f"Data updated successfully in table '{table_name}'")
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        logger.error(f"Error updating data in table '{table_name}': {str(e)}", exc_info=True)
        return json.dumps({"error": str(e), "details": traceback.format_exc()})

@mcp.tool(description="Delete rows from a PostgreSQL table that match a condition.")
async def delete_data(ctx: Context, table_name: str, condition: str, condition_params: List[Any]) -> str:
    """Delete rows from a table.
    
    Args:
        table_name: Target table
        condition: WHERE clause (e.g., "id = %s")
        condition_params: Parameters for the condition
    """
    logger.info(f"Tool called: delete_data(table_name='{table_name}', condition='{condition}', condition_params={condition_params})")
    db = ctx.request_context.lifespan_context.db
    try:
        result = await db.delete_data(table_name, condition, tuple(condition_params))
        logger.info(f"Data deleted successfully from table '{table_name}'")
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        logger.error(f"Error deleting data from table '{table_name}': {str(e)}", exc_info=True)
        return json.dumps({"error": str(e), "details": traceback.format_exc()})

@mcp.tool(description="Query data from a PostgreSQL table with filtering, sorting, and limiting options.")
async def select_data(
    ctx: Context,
    table_name: str, 
    columns: Optional[List[str]] = None,
    condition: Optional[str] = None, 
    condition_params: Optional[List[Any]] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = DEFAULT_LIMIT,
) -> str:
    """Select data from a table with filtering and sorting options.
    
    Args:
        table_name: Target table
        columns: List of columns to select (default: all columns)
        condition: WHERE clause (e.g., "status = %s")
        condition_params: Parameters for the condition
        order_by: ORDER BY clause (e.g., "created_at DESC")
        limit: Maximum number of rows to return
    """
    logger.info(f"Tool called: select_data(table_name='{table_name}', columns={columns}, condition='{condition}', condition_params={condition_params}, order_by='{order_by}', limit={limit})")
    db = ctx.request_context.lifespan_context.db
    try:
        condition_tuple = tuple(condition_params) if condition_params else None
        result = await db.select_data(table_name, columns, condition, condition_tuple, order_by, limit)
        rows_count = len(result) if result else 0
        logger.info(f"Selected {rows_count} rows from table '{table_name}'")
        return json.dumps(result, indent=2, default=str)
    except Exception as e:
        logger.error(f"Error selecting data from table '{table_name}': {str(e)}", exc_info=True)
        return json.dumps({"error": str(e), "details": traceback.format_exc()})

if __name__ == "__main__":
    # Run the server
    logger.info("Starting PostgreSQL MCP server")
    mcp.run(transport='stdio')