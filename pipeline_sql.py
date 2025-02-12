import os
import logging
from datetime import date, datetime

import psycopg2
import aiohttp
import asyncio
import json
from urllib.parse import urlparse
from typing import List, Union, Generator, Iterator
from jinja2 import Template  # For rendering HTML templates
import pandas as pd
from pydantic import BaseModel
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.DEBUG)


class Pipeline:
    class Valves(BaseModel):
        DB_URL: str
        DB_TABLES: List[str]
        OPENAI_API_KEY: str = ""
        PROMPT_TEMPLATE: str

    def __init__(self):
        self.name = "NLQ2SQL chart"
        self.conn = None

        self.valves = self.Valves(
            **{
                "DB_URL": os.getenv("DATABASE_URL", "postgresql://quocduy:quocduy@localhost:5432/vikki_data"),
                "DB_TABLES": ["nflow_core_server_1_case", "party_v2_public_customer", "vk_onboarding"],
                "PROMPT_TEMPLATE": self.get_prompt_template(),
                "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY","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"),

            }
        )

        self.init_db_connection()
        pass

    def get_prompt_template(self) -> str:
        """Returns a structured prompt template for better SQL generation."""
        return """
        You are an AI assistant specializing in SQL query generation. Convert the given natural language question (NLQ) into an optimized SQL query based on the table annotations provided.

        The database schema contains the following tables:

        CREATE TABLE nflow_core_server_1_case (
            documentcheck STRING COMMENT 'Verification status of user documents (e.g., autoApproved, autoDeclined, manuallyApproved, manuallyDeclined).',
            selfiecheck STRING COMMENT 'User’s selfie verification status (1=autoApproved, 2=autoDeclined, 3=manuallyApproved, 4=manuallyDeclined).',
            accuracycheck STRING COMMENT 'User’s data accuracy verification status.',
            idnumberdedup STRING COMMENT 'User ID number duplication check status.',
            nfccheck STRING COMMENT 'User NFC scan verification status.',
            facededupcheck STRING COMMENT 'User face data deduplication status.',
            amlcheck STRING COMMENT 'Anti-Money Laundering (AML) check status.',
            hdbcheck STRING COMMENT 'Data verification against trusted databases.',
            createdat DATE COMMENT 'Date and time of account creation.',
            onboardingid STRING COMMENT 'Foreign key associated with vk_onboarding.onboarding_id.'
        );

        CREATE TABLE vk_onboarding (
            id BIGINT,
            onboarding_id STRING COMMENT 'Primary key - ID of user onboarding.',
            start_time DATE COMMENT 'User’s onboarding journey start time.',
            end_time DATE COMMENT 'User’s onboarding journey end time.',
            onboarding_status STRING COMMENT 'Status of onboarding (SUCCESSFUL, PENDING, FAILED).',
            occupation STRING COMMENT 'User’s job code.',
            job_position STRING COMMENT 'Specific job position held by the user.',
            is_blacklist STRING COMMENT 'Indicates whether the user is blacklisted (Yes or No).'
        );

        CREATE TABLE party_v2_public_customer (
            cif_number STRING COMMENT 'User onboarding successful identifier.',
            onboarding_timestamp DATE COMMENT 'Time of successful onboarding.'
        );

        User Input: "{nl_query}"

        Provide the response in a structured JSON format: 
        {"query": "Generated SQL query"}
        """

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
        except Exception as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            raise

    async def call_openai(self, nlq: str) -> str:
        """Call OpenAI API to convert NLQ to SQL."""
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.valves.OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "gpt-3.5-turbo",
            "messages": [{"role": "system", "content": self.valves.PROMPT_TEMPLATE.replace("{nl_query}", nlq)}],
            "temperature": 0.3
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if "choices" in data and data["choices"]:
                        return json.dumps({"query": data["choices"][0]["message"]["content"].strip()})
                    return json.dumps({"error": "Invalid response from API"})
        except Exception as e:
            logging.error(f"Error calling OpenAI API: {e}")
            return json.dumps({"error": str(e)})

    def execute_sql_query(self, sql_query: str) -> Union[str, Generator, Iterator]:
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


    def pipe(self, user_message: str, model_id: str, messages: List[dict], body: dict):
        """Process a natural language query into SQL and return an HTML page with a chart."""
        if not self.conn:
            return "Database connection is not initialized."

        try:
            # Step 1: Generate SQL Query from NLQ
            sql_query = asyncio.run(self.call_openai(user_message))
            sql_dict = json.loads(sql_query) if isinstance(sql_query, str) else sql_query

            # Step 2: Extract the actual SQL query
            raw_query = sql_dict.get("query", "").strip()


            # Step 3: Execute the Query
            sql_result = self.execute_sql_query(raw_query)
            # Example column names (these should match your database schema)
            # Example column names (these should match your database schema)
            column_names = ["id", "user_id", "extra_data", "created_at", "updated_at", "status", "start_date",
                            "end_date", "occupation", "job_position"]

            # Adjust column names length to match data
            column_names += [f"col_{i}" for i in range(len(sql_result[0]) - len(column_names))]

            # Convert to list of dictionaries
            data_dicts = [dict(zip(column_names, row)) for row in sql_result]

            # Convert datetime and date objects to string for JSON serialization
            for entry in data_dicts:
                for key, value in entry.items():
                    if isinstance(value, (date, datetime)):
                        entry[key] = value.isoformat()

            # Convert to JSON
            json_data = json.dumps(data_dicts, indent=4)

            # Step 4: Convert to DataFrame
            df = pd.DataFrame(json_data)

            if df.empty:
                return "<h2>No data found for the query.</h2>"

            # Extract column names
            column_names = [desc[0] for desc in self.conn.cursor().description]
            df.columns = column_names

            # Convert DataFrame to JSON for JavaScript
            json_data = df.to_dict(orient="records")

            # Step 5: Render HTML with Chart.js
            html_template = """
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>SQL Query Results</title>
                <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; margin: 20px; }
                    canvas { max-width: 800px; margin: auto; }
                    table { width: 80%; margin: auto; border-collapse: collapse; }
                    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                    th { background-color: #4CAF50; color: white; }
                </style>
            </head>
            <body>
                <h2>Query Results Visualization</h2>
                <canvas id="chart"></canvas>
                <h3>Raw Data</h3>
                <table>
                    <tr>
                        {% for col in columns %}
                            <th>{{ col }}</th>
                        {% endfor %}
                    </tr>
                    {% for row in data %}
                    <tr>
                        {% for col in columns %}
                            <td>{{ row[col] }}</td>
                        {% endfor %}
                    </tr>
                    {% endfor %}
                </table>
                <script>
                    const jsonData = {{ json_data | tojson }};
                    const labels = jsonData.map(item => item["{{ columns[0] }}"]);
                    const values = jsonData.map(item => item["{{ columns[1] }}"]);

                    const ctx = document.getElementById("chart").getContext("2d");
                    new Chart(ctx, {
                        type: "bar",
                        data: {
                            labels: labels,
                            datasets: [{
                                label: "{{ columns[1] }}",
                                data: values,
                                backgroundColor: "rgba(54, 162, 235, 0.5)",
                                borderColor: "rgba(54, 162, 235, 1)",
                                borderWidth: 1
                            }]
                        },
                        options: {
                            responsive: true,
                            scales: {
                                y: { beginAtZero: true }
                            }
                        }
                    });
                </script>
            </body>
            </html>
            """

            # Step 6: Render HTML using Jinja2
            template = Template(html_template)
            rendered_html = template.render(columns=column_names, data=json_data, json_data=json_data)

            return rendered_html  # This will return an HTML page

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return f"<h2>Unexpected error: {e}</h2>"