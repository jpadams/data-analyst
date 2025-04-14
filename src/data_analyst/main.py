import pandas as pd
import duckdb
import os

import dagger
from dagger import dag, function, field, object_type


@object_type
class DataAnalyst:

    # transaction data
    # TRANSACTION_DATA_FILE_PATH = 'Store_Sales_Price_Elasticity_Promotions_Data.parquet'
    TRANSACTION_DATA_FILE_PATH = 's3://testdaggerevals/Store_Sales_Price_Elasticity_Promotions_Data.parquet'
    #TRANSACTION_DATA_FILE_PATH = '/src/**/testdaggerevals/Store_Sales_Price_Elasticity_Promotions_Data.parquet'


    @object_type
    class VisualizationConfig:
    # class defining the response format of step 1 of tool 3

        chart_type: str = field(..., description="Type of chart to generate", default="line")
        x_axis: str = field(..., description="Name of the x-axis column", default="date")
        y_axis: str = field(..., description="Name of the y-axis column", default="value")
        title: str = field(..., description="Title of the chart")


    # prompt template for step 2 of tool 1
    SQL_GENERATION_PROMPT = """
    Generate an SQL query based on a prompt. Do not reply with anything besides the SQL query.
    The prompt is: {prompt}

    The available columns are: {columns}
    The table name is: {table_name}
    """

    # Construct prompt based on analysis type and data subset
    DATA_ANALYSIS_PROMPT = """
    Analyze the following data: {data}
    Your job is to answer the following question: {prompt}
    """

    # prompt template for step 1 of tool 3
    CHART_CONFIGURATION_PROMPT = """
    Generate a chart configuration based on this data: {data}
    The goal is to show: {visualization_goal}
    """

    # prompt template for step 2 of tool 3
    CREATE_CHART_PROMPT = """
    Write python code to create a chart based on the following configuration.
    Only return the code, no other text.
    config: {config}
    """
    @classmethod
    async def create(cls):
        """Create an instance of DataAnalyst"""
        await dag.current_module().source().file("Store_Sales_Price_Elasticity_Promotions_Data.parquet").export("/foo")
        return cls()

    @function
    def ls(self, path: str) -> str:
        files = os.listdir(path)
        return files.__str__()
    
    # code for step 2 of tool 1
    @function
    async def generate_sql_query(self, prompt: str, columns: list[str], table_name: str) -> str:
        """Generate an SQL query based on a prompt"""
        formatted_prompt = self.SQL_GENERATION_PROMPT.format(prompt=prompt, columns=columns, table_name=table_name)
        return await (
            dag
            .llm()
            .with_prompt(formatted_prompt)
            .last_reply()
        )
  
    # code for tool 1
    @function
    async def lookup_sales_data(self, prompt: str) -> str:
        """Implementation of sales data lookup from parquet file using SQL"""
        try:
    
            # define the table name
            table_name = "sales"
    
            # step 1: read the parquet file into a DuckDB table
            df = pd.read_parquet(self.TRANSACTION_DATA_FILE_PATH)
            duckdb.sql(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df")
    
            # step 2: generate the SQL code
            sql_query = await self.generate_sql_query(prompt, df.columns, table_name)
            # clean the response to make sure it only includes the SQL code
            sql_query = sql_query.strip()
            sql_query = sql_query.replace("```sql", "").replace("```", "")
    
            # step 3: execute the SQL query
            result = duckdb.sql(sql_query).df()
    
            return result.to_string()
        except Exception as e:
            return f"Error accessing data: {str(e)}"

    # code for tool 2
    @function
    async def analyze_sales_data(self, prompt: str, data: str) -> str:
        """Implementation of AI-powered sales data analysis"""
        formatted_prompt = self.DATA_ANALYSIS_PROMPT.format(data=data, prompt=prompt)

        analysis = await (
            dag
            .llm()
            .with_prompt(formatted_prompt)
            .last_reply()
        )
        return analysis if analysis else "No analysis could be generated"
    

    # # code for step 1 of tool 3
    # @function
    # async def extract_chart_config(self, data: str, visualization_goal: str) -> dict:
    #     """Generate chart visualization configuration
    
    #      Args:
    #         data: String containing the data to visualize
    #         visualization_goal: Description of what the visualization should show
        
    #     Returns:
    #          Dictionary containing line chart configuration
    #     """
    #     formatted_prompt = self.CHART_CONFIGURATION_PROMPT.format(data=data,
    #                                                      visualization_goal=visualization_goal)
    
    #     #with_visualization_config_output("json", "generated visualization config")

    #     content = await (
    #         dag
    #         .llm()
    #         .with_env(vis_env)
    #         .with_prompt(formatted_prompt)
    #         .last_reply()
    #     )
    #     response = client.beta.chat.completions.parse(
    #         model=MODEL,
    #        messages=[{"role": "user", "content": formatted_prompt}],
    #        response_format=VisualizationConfig,
    #     )
    
    #     try:
    #         # Extract axis and title info from response
    #         content = response.choices[0].message.content
            
    #         # Return structured chart config
    #         return {
    #             "chart_type": content.chart_type,
    #             "x_axis": content.x_axis,
    #             "y_axis": content.y_axis,
    #             "title": content.title,
    #             "data": data
    #         }
    #     except Exception:
    #         return {
    #             "chart_type": "line", 
    #             "x_axis": "date",
    #             "y_axis": "value",
    #             "title": visualization_goal,
    #             "data": data
    #         }
        
    #     # code for step 2 of tool 3
    #     @function
    #     def create_chart(self, config: dict) -> str:
    #         """Create a chart based on the configuration"""
    #         formatted_prompt = self.CREATE_CHART_PROMPT.format(config=config)
            
    #         response = client.chat.completions.create(
    #             model=MODEL,
    #             messages=[{"role": "user", "content": formatted_prompt}],
    #         )
            
    #         code = response.choices[0].message.content
    #         code = code.replace("```python", "").replace("```", "")
    #         code = code.strip()
            
    #         return code

    #     # code for tool 3
    #     @function
    #     def generate_visualization(self, data: str, visualization_goal: str) -> str:
    #         """Generate a visualization based on the data and goal"""
    #         config = extract_chart_config(data, visualization_goal)
    #         code = create_chart(config)
    #         return code
