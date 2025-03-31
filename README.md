# PostgreSQL MCP Server (Model Context Protocol)

A basic implementation of FastMCP for PostgreSQL, enabling direct interaction with PostgreSQL databases from Claude AI.

With this MCP, you can transform your AI chat experience by:
- Storing and retrieving chat data in PostgreSQL databases
- Creating AI applications with persistent data storage
- Building knowledge management systems with structured database queries
- Analyzing large datasets directly through natural language prompts
- Implementing database-driven workflows without writing traditional code

This implementation leverages the MCP protocol to securely bridge Claude AI with PostgreSQL databases, allowing Claude to execute operations on your behalf while maintaining proper isolation and security boundaries.

## Features

- Query execution against PostgreSQL databases
- Table management (create, drop)
- Data operations (select, insert, update, delete)
- Schema inspection
- Integrated with Claude through MCP protocol

## Prerequisites

- Python 3.8+
- PostgreSQL server
- Access to Claude AI with MCP capabilities

## Installation

1. Clone this repository to your local machine

2. Create and activate a Python virtual environment:

```bash
# Create virtual environment
python -m venv .mcp

# Activate virtual environment
# On macOS/Linux
source .mcp/bin/activate
# On Windows
.mcp\Scripts\activate
```

3. Install required dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

1. Create a `.env` file in the project root with your PostgreSQL connection details and debugging:

2. Configure the PostgreSQL MCP with Claude AI app by adding the following configuration:

```json
{
  "mcpServers": {
    "PostgreSQL MCP": {
      "command": "<path/to/clonedrepo/.mcp/bin/uv",
      "args": [
        "run",
        "--with",
        "mcp[cli]",
        "--with",
        "asyncpg",
        "--with",
        "httpx",
        "--with",
        "python-dotenv",
        "--with",
        "psycopg2-binary",
        "mcp",
        "run",
        "path/to/clonedrepo/postgres_mcp_server.py"
      ],
      "env": {
        "POSTGRES_HOST": "<your_postgres_host>",
        "POSTGRES_PORT": "<your_postgres_port>",
        "POSTGRES_USER": "<your_username>",
        "POSTGRES_PASSWORD": "<your_password>",
        "POSTGRES_DB": "<your_database_name>"
      }
    }
  }
}
```
Note : Replace "path/to/clonedrepo/" with actual path

Add this configuration to the Claude AI app settings in the MCP configuration section. This will allow Claude to connect to your PostgreSQL MCP server.
