import asyncio
import asyncpg
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

class PostgresManager:
    """Manager for PostgreSQL database operations."""
    
    def __init__(self, host: str, port: str, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.pool = None
        
    async def connect(self):
        """Establish connection pool to PostgreSQL."""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            logger.info(f"Connected to PostgreSQL: {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
            
    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")
    
    async def get_tables(self) -> List[str]:
        """Get all tables in the database."""
        async with self.pool.acquire() as conn:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """
            rows = await conn.fetch(query)
            return [row['table_name'] for row in rows]
    
    async def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """Get schema information for a specific table."""
        async with self.pool.acquire() as conn:
            query = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = 'public'
                ORDER BY ordinal_position
            """
            rows = await conn.fetch(query, table_name)
            return [dict(row) for row in rows]
    
    async def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a raw SQL query."""
        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(query)
                return [dict(row) for row in rows]
            except Exception as e:
                logger.error(f"Query execution error: {str(e)}")
                raise
    
    async def select_data(
        self, 
        table_name: str, 
        columns: Optional[List[str]] = None,
        condition: Optional[str] = None, 
        condition_params: Optional[Tuple[Any, ...]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = 100,
    ) -> List[Dict[str, Any]]:
        """Select data from a table with filtering and sorting options."""
        cols_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols_str} FROM {table_name}"
        
        params = []
        if condition:
            query += f" WHERE {condition}"
            if condition_params:
                params.extend(condition_params)
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]
    
    async def create_table(self, table_name: str, columns: List[Dict[str, str]]) -> None:
        """Create a new table with specified columns.
        
        Args:
            table_name: Name of the table to create
            columns: List of column definitions, each with 'name' and 'type' keys
        """
        column_defs = []
        for column in columns:
            column_defs.append(f"{column['name']} {column['type']}")
        
        columns_sql = ", ".join(column_defs)
        query = f"CREATE TABLE {table_name} ({columns_sql})"
        
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(query)
                logger.info(f"Created table: {table_name}")
            except Exception as e:
                logger.error(f"Error creating table {table_name}: {str(e)}")
                raise
    
    async def drop_table(self, table_name: str) -> None:
        """Drop a table from the database.
        
        Args:
            table_name: Name of the table to drop
        """
        query = f"DROP TABLE {table_name}"
        
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(query)
                logger.info(f"Dropped table: {table_name}")
            except Exception as e:
                logger.error(f"Error dropping table {table_name}: {str(e)}")
                raise
    
    async def insert_data(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a row into a table.
        
        Args:
            table_name: Target table
            data: Dictionary with column-value pairs
            
        Returns:
            The inserted row or information about the insert operation
        """
        columns = list(data.keys())
        values = list(data.values())
        
        placeholders = [f"${i+1}" for i in range(len(values))]
        
        columns_str = ", ".join(columns)
        placeholders_str = ", ".join(placeholders)
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders_str}) RETURNING *"
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchrow(query, *values)
                return dict(result) if result else {"status": "success", "message": "Data inserted"}
            except Exception as e:
                logger.error(f"Error inserting data into {table_name}: {str(e)}")
                raise
    
    async def update_data(self, table_name: str, data: Dict[str, Any], condition: str, condition_params: Tuple[Any, ...]) -> Dict[str, Any]:
        """Update rows in a table.
        
        Args:
            table_name: Target table
            data: Dictionary with column-value pairs to update
            condition: WHERE clause with placeholders (e.g., "id = %s")
            condition_params: Values for the placeholders in the condition
            
        Returns:
            Information about the update operation
        """
        # Convert PostgreSQL-style placeholders (e.g., %s) in the condition to asyncpg-compatible ones
        modified_condition = condition
        if '%s' in condition:
            for i in range(condition.count('%s')):
                modified_condition = modified_condition.replace('%s', f'${i+len(data)+1}', 1)
        
        set_clauses = []
        values = []
        
        for i, (column, value) in enumerate(data.items(), start=1):
            set_clauses.append(f"{column} = ${i}")
            values.append(value)
        
        set_clause = ", ".join(set_clauses)
        query = f"UPDATE {table_name} SET {set_clause} WHERE {modified_condition} RETURNING *"
        
        all_params = values + list(condition_params)
        
        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(query, *all_params)
                updated_rows = [dict(row) for row in rows]
                return {
                    "status": "success", 
                    "rows_updated": len(updated_rows),
                    "updated_data": updated_rows
                }
            except Exception as e:
                logger.error(f"Error updating data in {table_name}: {str(e)}")
                raise
    
    async def delete_data(self, table_name: str, condition: str, condition_params: Tuple[Any, ...]) -> Dict[str, Any]:
        """Delete rows from a table.
        
        Args:
            table_name: Target table
            condition: WHERE clause with placeholders (e.g., "id = %s")
            condition_params: Values for the placeholders in the condition
            
        Returns:
            Information about the delete operation
        """
        # Convert PostgreSQL-style placeholders in the condition to asyncpg-compatible ones
        modified_condition = condition
        if '%s' in condition:
            for i in range(condition.count('%s')):
                modified_condition = modified_condition.replace('%s', f'${i+1}', 1)
        
        query = f"DELETE FROM {table_name} WHERE {modified_condition} RETURNING *"
        
        async with self.pool.acquire() as conn:
            try:
                rows = await conn.fetch(query, *condition_params)
                deleted_rows = [dict(row) for row in rows]
                return {
                    "status": "success", 
                    "rows_deleted": len(deleted_rows),
                    "deleted_data": deleted_rows
                }
            except Exception as e:
                logger.error(f"Error deleting data from {table_name}: {str(e)}")
                raise
