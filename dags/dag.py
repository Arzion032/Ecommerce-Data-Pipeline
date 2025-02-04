from airflow.decorators import dag, task
from datetime import datetime
from extract.get_data_kaggle import extract_from_kaggle
from load.load_data_snowflake import dump_data
import sys
import os

# Add 'extract' folder to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'extract'))

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 1)
}

@dag(
    schedule_interval="@daily",  
    default_args=default_args,   
    description='E-Commerce Data Pipeline Project' 
)
def ecommerce_data_pipeline():

    @task()
    def extract():
        """This function will extract data from Kaggle and convert it."""
        extract_from_kaggle()  
    
    @task()
    def load():
        dump_data()
        
    # Call the extraction and conversion task
    extract() >> load()

# Instantiate the DAG
ecommerce_data_pipeline_dag = ecommerce_data_pipeline()
