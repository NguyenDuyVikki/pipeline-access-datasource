import os
import logging
import traceback
from fastapi.responses import HTMLResponse
from jinja2 import Template
import psycopg2
import aiohttp
import asyncio
import json
from urllib.parse import urlparse
from typing import List, Union, Any

from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

# from dotenv import load_dotenv
# load_dotenv()
logging.basicConfig(level=logging.DEBUG)


class Pipeline:
    class Valves(BaseModel):
        DB_URL: str
        DB_TABLES: List[str]
        OPENAI_API_KEY: str = ""
        show_status: bool = Field(
            default=True, description="Show status of the action."
        )
        html_filename: str = Field(
            default="json_visualizer.html",
            description="Name of the HTML file to be created or retrieved.",
        )


        # def __init__(self):
        #     self.DB_URL = os.getenv("DATABASE_URL", "postgresql://quocduy:quocduy@localhost:5432/vikki_data")
        #     # self.DB_URL = os.getenv("DATABASE_URL", "postgresql://quocduy:quocduy@host.docker.internal:5432/vikki_data")
        #
        #     self.DB_TABLES = ["nflow_core_server_1_case", "party_v2_public_customer", "vk_onboarding"]
        #     self.OPENAI_API_KEY = os.getenv("OPENAI_API_KEY","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")
        #     self.TEXT2SQL_TEMPLATE = self.get_prompt_template("Text2Sql")
        #     self.VISUALIZE_TEMPLATE = self.get_prompt_template("DataVisualize")


    def get_prompt_template(self, prompt_type: str) -> str:
            """Returns a structured prompt template for SQL generation or data visualization."""
            if prompt_type == "Text2Sql":
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

            elif prompt_type == "DataVisualize":
                return """
                     You are a data analysis expert, and now you need to choose the appropriate visualization format based on the user's questions and data.
                     There are four display types in total: table, bar, pie, and line. The output format is in JSON format.
                     The fields are as follows:
                     show_type: The type of display
                     format_data: The specific data

                     <instructions>
                     - The format of format_data is a nested structure of a list, with the first element being the column name.
                     - If there are more than 3 column queries, show_type is table.
                     - If there are two columns, show_type needs to be selected from the appropriate types of table, bar, pie, and line based on the data situation.
                     - If show_type is bar, pie, or line, the first column is the x-axis and the second column is the y-axis.
                     - If show_type is table, the number of columns in format_data can exceed 2.
                     - Only output JSON format, no other comments.
                     </instructions>

                     <example>

                     Question: How many male and female users have completed the purchase?

                     The example data is: [['num_users', 'gender'], [1906, 'F'], [1788, 'M']]

                     The answer is:

                     ```json
                     {{
                         "show_type": "pie",
                         "format_data": [["gender", "num_users"], ["F", 1906], ["M", 1788]]
                     }}
                     ```
                     </example>

                     The user question is: {question}
                     The data is: {data}
                     """
            elif prompt_type == "Chart":
                return """

                Objective:
                Your goal is to read the query, extract the data, choose the appropriate chart to present the data, and produce the HTML to display it.

                Steps:

                	1.	Read and Examine the Query:
                	•	Understand the user’s question and identify the data provided.
                	2.	Analyze the Data:
                	•	Examine the data in the query to determine the appropriate chart type (e.g., bar chart, pie chart, line chart) for effective visualization.
                	3.	Generate HTML:
                	•	Create the HTML code to present the data using the selected chart format.
                	4.	Handle No Data Situations:
                	•	If there is no data in the query or the data cannot be presented as a chart, generate a humorous or funny HTML response indicating that the data cannot be presented.
                    5.	Calibrate the chart scale based on the data:
                	•	based on the data try to make the scale of the chart as readable as possible.

                Key Considerations:

                	-	Your output should only include HTML code, without any additional text.
                    -   Generate only HTML. Do not include any additional words or explanations.
                    -   Make to remove any character other non alpha numeric from the data.
                    -   is the generated HTML Calibrate the chart scale based on the data for eveything to be readable.
                    -   Generate only html code , nothing else , only html.


                Example1 : 
                '''
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Interactive Chart</title>
                    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                </head>
                <body>
                    <div id="chart" style="width: 100%; height: 100vh;"></div>
                    <button id="save-button">Save Screenshot</button>
                    <script>
                        // Data for the chart
                        var data = [{
                            x: [''Category 1'', ''Category 2'', ''Category 3''],
                            y: [20, 14, 23],
                            type: ''bar''
                        }];

                        // Layout for the chart
                        var layout = {
                            title: ''Interactive Bar Chart'',
                            xaxis: {
                                title: ''Categories''
                            },
                            yaxis: {
                                title: ''Values''
                            }
                        };

                        // Render the chart
                        Plotly.newPlot(''chart'', data, layout);

                        // Function to save screenshot
                        document.getElementById(''save-button'').onclick = function() {
                            Plotly.downloadImage(''chart'', {format: ''png'', width: 800, height: 600, filename: ''chart_screenshot''});
                        };

                        // Function to update chart attributes
                        function updateChartAttributes(newData, newLayout) {
                            Plotly.react(''chart'', newData, newLayout);
                        }

                        // Example of updating chart attributes
                        var newData = [{
                            x: [''New Category 1'', ''New Category 2'', ''New Category 3''],
                            y: [10, 22, 30],
                            type: ''bar''
                        }];

                        var newLayout = {
                            title: ''Updated Bar Chart'',
                            xaxis: {
                                title: ''New Categories''
                            },
                            yaxis: {
                                title: ''New Values''
                            }
                        };

                        // Call updateChartAttributes with new data and layout
                        // updateChartAttributes(newData, newLayout);
                    </script>
                </body>
                </html>
                '''

                Example2:
                '''
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Collaborateurs par Métier/Fonction</title>
                    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                </head>
                <body>
                    <div id="myChart" style="width: 100%; max-width: 700px; height: 500px; margin: 0 auto;"></div>
                    <script>
                        var data = [{
                            x: ["Ingénieur Système", "Solution Analyst", "Ingénieur d''études et Développement", "Squad Leader", "Architecte d''Entreprise", "Tech Lead", "Architecte Technique", "Référent Méthodes / Outils"],
                            y: [5, 3, 2, 1, 1, 1, 1, 1],
                            type: "bar",
                            marker: {
                                color: "rgb(49,130,189)"
                            }
                        }];
                        var layout = {
                            title: "Collaborateurs de STT par Métier/Fonction",
                            xaxis: {
                                title: "Métier/Fonction"
                            },
                            yaxis: {
                                title: "Nombre de Collaborateurs"
                            }
                        };
                        Plotly.newPlot("myChart", data, layout);
                    </script>
                </body>
                </html>
                '''

                2.	No Data or Unchartable Data:
                ''' 
                <html>
                <body>
                    <h1>We''re sorry, but your data can''t be charted.</h1>
                    <p>Maybe try feeding it some coffee first?</p>
                    <img src="https://media.giphy.com/media/l4EoTHjkw0XiYtNRG/giphy.gif" alt="Funny Coffee GIF">
                </body>
                </html>

                '''
                User Input: "{nl_query}"

                Provide the response in a structured JSON format: 
                {"response": "chart response"}
                """

            else:
                raise ValueError(
                    f"Invalid prompt_type: {prompt_type}. Supported types are 'Text2Sql' and 'DataVisualize'.")

    def __init__(self):
        self.name = "Chart"
        self.conn = None
        self.valves = self.Valves(
            **{
                "DB_URL": os.getenv("DATABASE_URL", "postgresql://quocduy:quocduy@localhost:5432/vikki_data"),
                "DB_TABLES": ["nflow_core_server_1_case", "party_v2_public_customer", "vk_onboarding"],
                "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY","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"),
                "html_content": """ """
            }
        )
        self.init_db_connection()


    def init_db_connection(self):
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

    async def call_openai(self, nlq: str, prompt_type: str) -> str:
        """Call OpenAI API to convert NLQ to SQL."""
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.valves.OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        formatter_prompt = prompt_type.replace("nl_query", nlq)
        payload = {
            "model": "gpt-4-turbo",
            "messages": [{"role": "system", "content": formatter_prompt}],
            "temperature": 0.3
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as response:
                    if response.status != 200:
                        error_msg = await response.text()
                        raise Exception(f"OpenAI API error: {response.status} - {error_msg}")
                    result = await response.json()
                    return result.get("choices", [{}])[0].get("message", {}).get("content", "")
        except Exception as e:
            logging.error(f"Error calling OpenAI API: {e}")
            return json.dumps({"error": str(e)})

    def execute_sql_query(self, sql_query: str) -> str | tuple[Any, list[Any]]:
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
                column_names = [desc[0] for desc in cursor.description]
                return result, column_names

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
            sql_query =  asyncio.run(self.call_openai(user_message, self.get_prompt_template("Text2Sql")))
            try:
                # Attempt to parse the response as JSON
                if isinstance(sql_query, str):
                    sql_dict = json.loads(sql_query.strip())  # Remove extra whitespace and parse JSON
                else:
                    raise ValueError("Invalid SQL query format received from OpenAI API.")

                # Ensure the "query" key exists
                if "query" not in sql_dict:
                    raise KeyError("Missing 'query' key in OpenAI API response.")

                # Step 2: Execute the SQL Query
                sql_result = self.execute_sql_query(sql_dict["query"])
            except json.JSONDecodeError as e:
                raise ValueError(f"Failed to parse SQL query response: {e}")

            except KeyError as e:
                raise ValueError(f"Unexpected response format: {e}")

            except Exception as e:
                raise RuntimeError(f"Error processing SQL query: {e}")
            if isinstance(sql_result, str):  # If error, return it
                return sql_result

            rows, column_names = sql_result
            if not rows:
                return "<h2>No data found for the query.</h2>"

            # Step 3: Convert SQL Result to JSON
            data_dicts = [dict(zip(column_names, row)) for row in rows]
            json_data = json.dumps(data_dicts, default= str)
            html_chart =  asyncio.run(self.call_openai(json_data, self.get_prompt_template("Chart")))

            return html_chart


        except Exception as e:
            traceback.print_exc()


