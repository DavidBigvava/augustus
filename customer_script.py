import os
import pandas as pd
import logging
from langchain_community.llms import Ollama
from mistralai import Mistral

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the Data Catalog
DATA_CATALOG = {
    "sales": {
        "description": "Main sales transactions table",
        "source": {
            "path": "retail_sales.xlsx",
            "type": "excel",
            "sheet": "sales"
        },
        "columns": {
            "ID": "Unique transaction identifier",
            "ship_mode_id": "Foreign key to ship_mode.id",
            "Sales": "Dollar amount of the sale",
        },
        "relationships": [
            {
                "target_table": "ship_mode",
                "source_column": "ship_mode_id",
                "target_column": "id"
            }
        ]
    },
    "ship_mode": {
        "description": "Shipping modes reference table",
        "source": {
            "path": "retail_sales.xlsx",
            "type": "excel",
            "sheet": "ship_mode"
        },
        "columns": {
            "id": "Primary key identifier",
            "ship_mode": "Name of shipping mode"
        },
        "relationships": []
    }
}

class DataManager:
    def __init__(self, catalog):
        self.catalog = catalog
        self.tables = self._load_all_tables()
        self.merged_df = None

    def _load_all_tables(self):
        """Load all tables from the data catalog into memory"""
        tables = {}
        for table_name, config in self.catalog.items():
            source = config["source"]
            try:
                if source["type"] == "excel":
                    df = pd.read_excel(
                        source["path"], 
                        sheet_name=source["sheet"],
                        engine='openpyxl'
                    )   
                elif source["type"] == "csv":
                    df = pd.read_csv(source["path"])
                tables[table_name] = df
                logger.info(f"Successfully loaded table: {table_name}")
            except Exception as e:
                logger.error(f"Error loading {table_name}: {str(e)}")
                raise
        return tables

    def _find_join_path(self, target_tables):
        """Determine how to join tables based on relationships"""
        join_path = []
        visited = set()
        
        def dfs(current_table):
            if current_table in visited:
                return
            visited.add(current_table)
            for relationship in self.catalog[current_table]["relationships"]:
                if relationship["target_table"] in target_tables:
                    join_path.append({
                        "left_table": current_table,
                        "right_table": relationship["target_table"],
                        "left_col": relationship["source_column"],
                        "right_col": relationship["target_column"]
                    })
                dfs(relationship["target_table"])
        
        for table in target_tables:
            dfs(table)
            
        return join_path

    def merge_tables(self, query):
        """Dynamically merge tables needed to answer the query"""
        target_tables = ["sales", "ship_mode"]  # Simplified for this example
        
        join_path = self._find_join_path(target_tables)
        
        if not join_path:
            return self.tables[target_tables[0]]
        
        merged_df = self.tables[join_path[0]["left_table"]].copy()
        
        for join in join_path:
            right_df = self.tables[join["right_table"]]
            merged_df = merged_df.merge(
                right_df,
                left_on=join["left_col"],
                right_on=join["right_col"],
                how="left"
            )
            
        return merged_df

class LLMClient:
    def __init__(self):
        self.api_key = os.environ.get("MISTRAL_API_KEY")
        self.client = None
        self.local_llm = None
        
        if self.api_key:
            try:
                self.client = Mistral(api_key=self.api_key)
                logger.info("Initialized Mistral API client")
            except Exception as e:
                logger.error(f"Failed to initialize Mistral API: {str(e)}")
                raise
        else:
            try:
                self.local_llm = Ollama(model="mistral")
                logger.info("Initialized local Ollama client")
            except Exception as e:
                logger.error(f"Failed to initialize local LLM: {str(e)}")
                raise

    def generate(self, prompt):
        """Handle both API and local LLM responses"""
        if self.client:
            response = self.client.chat.complete(
                model="mistral-large-latest",
                messages=[{"role": "user", "content": prompt}]
            )
            return response.choices[0].message.content
        else:
            return self.local_llm(prompt)

# Initialize components
try:
    logger.info("Initializing system components...")
    llm_client = LLMClient()
    data_manager = DataManager(DATA_CATALOG)
    logger.info("All components initialized successfully")
except Exception as e:
    logger.error(f"Initialization failed: {str(e)}")
    raise

def process_input(user_input):
    """Main processing function for user queries"""
    try:
        logger.info(f"Processing query: {user_input}")
        
        # Merge tables based on query
        merged_data = data_manager.merge_tables(user_input)
        
        # Generate a prompt for the LLM
        prompt = f"""
        You are a data analyst. Given the following data, answer the user's query.
        
        Data:
        {merged_data.head().to_string()}
        
        Query:
        {user_input}
        
        Provide a clear and concise response.
        """
        
        # Get result from LLM
        result = llm_client.generate(prompt)
        
        # Format output
        return str(result)
    
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return f"Error processing your request: {str(e)}"

# Example usage
if __name__ == "__main__":
    query = "Show total sales by shipping modes"
    print(process_input(query))