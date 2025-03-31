# PostgreSQL MCP (Minimum Capability Provider)

A basic implementation of FastMCP for PostgreSQL, enabling direct interaction with PostgreSQL databases from Claude AI.

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
pip install mcp[cli] asyncpg httpx python-dotenv psycopg2-binary
```

## Configuration

1. Create a `.env` file in the project root with your PostgreSQL connection details and debuddhing:
