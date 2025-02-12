import psycopg2
from psycopg2 import sql
import logging
import os
from urllib.parse import urlparse
from typing import List, Union, Generator, Iterator
from pydantic import BaseModel
import aiohttp
import asyncio

logging.basicConfig(level=logging.DEBUG)

class Pipeline:
    class Valves(BaseModel):
        DB_URL: str
        DB_TABLES: List[str]

    def __init__(self):
        self.name = "Database Query"
        self.conn = None
        self.nlsql_response = ""

        self.valves = self.Valves(
            **{
                "DB_URL": os.getenv("DATABASE_URL", "postgresql://quocduy:quocduy@localhost:5432/vikki_data"),
                "DB_TABLES": ["nflow_core_server_1_case", "party_v2_public_customer", "vk_onboarding"],
            }
        )

        self.init_db_connection()

    def init_db_connection(self):
        """Initialize database connection using DATABASE_URL."""
        try:
            db_url = self.valves.DB_URL
            result = urlparse(db_url)

            if not all([result.scheme, result.hostname, result.path, result.username, result.password]):
                raise ValueError("Invalid DATABASE_URL format")

            # Extract credentials
            connection_params = {
                "dbname": result.path.lstrip("/"),  # Remove leading slash
                "user": result.username,
                "password": result.password,
                "host": result.hostname,
                "port": result.port or 5432  # Default to PostgreSQL's default port if not provided
            }

            # Establish connection
            self.conn = psycopg2.connect(**connection_params)
            self.conn.autocommit = True
            logging.info("Connected to PostgreSQL successfully.")

            # List tables in the database
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

    async def on_startup(self):
        """Startup event to initialize database connection."""
        self.init_db_connection()

    async def make_request_with_retry(self, url, params, retries=3, timeout=10):
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=timeout) as response:
                        response.raise_for_status()
                        return await response.text()
            except (aiohttp.ClientResponseError, aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                logging.error(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt + 1 == retries:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    async def on_shutdown(self):
        """Shutdown event to close database connection."""
        if self.conn:
            self.conn.close()
            logging.info("PostgreSQL connection closed.")

    async def make_request_with_retry(self, url, params, retries=3, timeout=10):
        """Make an HTTP request with retries."""
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=timeout) as response:
                        response.raise_for_status()
                        return await response.text()
            except (aiohttp.ClientResponseError, aiohttp.ClientPayloadError, aiohttp.ClientConnectionError) as e:
                logging.error(f"Attempt {attempt + 1} failed with error: {e}")
                if attempt + 1 == retries:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict) -> Union[str, Generator, Iterator]:
        """Execute an SQL query securely."""
        if not self.conn:
            return "Database connection is not initialized."

        try:
            with self.conn.cursor() as cursor:
                query = user_message.strip()

                # Ensure only SELECT queries are executed
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
