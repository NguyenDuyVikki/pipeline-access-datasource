import psycopg2
from psycopg2 import sql
import logging
import os
from urllib.parse import urlparse
from typing import List, Union, Generator, Iterator
from pydantic import BaseModel
import aiohttp
import asyncio
import json

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        DB_URL: str
        DB_TABLES: List[str]
        OLLAMA_BASE_URL: str  # Using Ollama with DeepSeek-R1 7B

    def __init__(self):
        self.name = "NLQ to SQL Pipeline"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "DB_URL": os.getenv("DATABASE_URL", "postgresql://quocduy:quocduy@localhost:5432/vikki_data"),
                "DB_TABLES": ["nflow_core_server_1_case", "party_v2_public_customer", "vk_onboarding"],
                "OLLAMA_BASE_URL": os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")  # DeepSeek-R1 7B model running on Ollama
            }
        )

        # Define Prompt Template Correctly
        self.prompt_template = """
        You are an expert in SQL query generation and data visualization. Your task is to convert a given **natural language question (NLQ)** into an **optimized SQL query** based on the provided **database schema**.

        ### **Step 1: SQL Query Generation**
        - Ensure correct SQL syntax and query optimization.
        - Identify the correct tables and column names.
        - Apply necessary **filters (`WHERE`), grouping (`GROUP BY`), ordering (`ORDER BY`)**, and joins (`JOIN`) if required.
        - Use **indexes, aggregations (`SUM`, `COUNT`, `AVG`), and LIMIT clauses** when needed.
        - Convert **date/time fields** into the proper format.
        - Use **subqueries or CTEs** for complex queries.
        - Handle **NULL values** correctly (`COALESCE`, `IFNULL`, or `CASE` when required).

        ### **Step 2: Chart Metadata for Data Visualization**
        - Provide structured chart metadata for visualization.
        - Suggest the best **chart type** (Line Chart, Bar Chart, Pie Chart, Scatter Plot, Histogram).
        - Define **X-axis & Y-axis values**.
        - Include **grouping and aggregation logic** (e.g., per day, per category).
        - Set **filters & conditions** (e.g., last 30 days, specific user status).
        - Specify **sorting order** (ascending/descending).

        ### **Example Output Format**
        ```json
        {
            "sql_query": "SELECT DATE(onboarding_timestamp) AS day, COUNT(cif_number) AS total_users FROM test.party GROUP BY DATE(onboarding_timestamp) ORDER BY day;",
            "chart_suggestion": {
                "chart_type": "Line Chart",
                "x_axis": "day",
                "y_axis": "total_users",
                "description": "This chart shows the trend of user onboarding over time."
            }
        }
        ```

        ### **User Input:**
        "{nl_query}"
        """

        self.init_db_connection()

    def init_db_connection(self):
        """Initialize database connection using DATABASE_URL."""
        try:
            db_url = self.valves.DB_URL
            result = urlparse(db_url)

            if not all([result.scheme, result.hostname, result.path, result.username, result.password]):
                raise ValueError("Invalid DATABASE_URL format")

            connection_params = {
                "dbname": result.path.lstrip("/"),
                "user": result.username,
                "password": result.password,
                "host": result.hostname,
                "port": result.port or 5432
            }

            self.conn = psycopg2.connect(**connection_params)
            self.conn.autocommit = True
            logging.info("Connected to PostgreSQL successfully.")
            self.list_tables()

        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            raise

    def list_tables(self):
        """List tables in the connected database."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                    AND table_schema NOT IN ('information_schema', 'pg_catalog');
                """)
                tables = cur.fetchall()
                logging.info("Tables in the database:")
                for schema, table in tables:
                    logging.info(f"{schema}.{table}")

        except Exception as e:
            logging.error(f"Error fetching tables: {e}")

    async def call_ollama(self, nlq: str) -> str:
        """Call Ollama API with DeepSeek-R1 7B to convert NLQ to SQL."""
        url = f"{self.valves.OLLAMA_BASE_URL}/api/generate"
        headers = {"Content-Type": "application/json"}
        payload = {
            "model": "deepseek-r1:7b",  # Specify the model name correctly
            "prompt": self.prompt_template.replace("{nl_query}", nlq),
            "stream": False
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=json.dumps(payload)) as response:
                    response.raise_for_status()
                    data = await response.json()
                    sql_query = data["response"].strip()
                    logging.info(f"DeepSeek-R1 7B SQL Output: {sql_query}")
                    return sql_query
        except Exception as e:
            logging.error(f"Error calling DeepSeek-R1 7B API: {e}")
            return "ERROR"

    def execute_sql_query(self, sql_query: str) -> Union[str, Generator, Iterator]:
        """Execute an SQL query securely."""
        if not self.conn:
            return "Database connection is not initialized."

        try:
            with self.conn.cursor() as cursor:
                query = sql_query.strip()

                if not query.lower().startswith("select"):
                    return "Only SELECT queries are allowed for security reasons."

                cursor.execute(query)
                result = cursor.fetchall()
                return str(result)

        except psycopg2.Error as e:
            logging.error(f"PostgreSQL Error: {e}")
            return f"PostgreSQL Error: {e}"
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return f"Unexpected error: {e}"

    async def pipeline(self, nlq: str) -> str:
        """Full pipeline: Convert NLQ to SQL using DeepSeek-R1 7B, then execute the SQL query."""
        logging.info(f"Received NLQ: {nlq}")

        sql_query = await self.call_ollama(nlq)

        if sql_query == "ERROR":
            return "Error in NLQ to SQL conversion."

        result = self.execute_sql_query(sql_query)
        return result
