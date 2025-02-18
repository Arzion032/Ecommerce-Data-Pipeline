from airflow.decorators import dag, task
from datetime import datetime
from extract.get_data_kaggle import extract_from_kaggle
from load.load_data_snowflake import dump_data

from transform.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from airflow.operators.email_operator import EmailOperator
from datetime import timedelta

import sys
import os
import subprocess

# Add 'extract' folder to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), 'extract'))

# Default arguments for the DAG
default_args = {
    "owner": "Melvin",
    "start_date": datetime(2025, 2, 1),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "email_subject": "E-Commerce Data Pipeline",
    "email_recipients": ["jmelvinsarabia032@gmail.com"]  # Replace with your email address
}

@dag(
    schedule_interval="@daily",  
    default_args=default_args,   
    description='E-Commerce Data Pipeline Project' ,
    catchup= False
)
def ecommerce_data_pipeline():

    @task()
    def extract():
        """This function will extract data from Kaggle and convert it from csv to parquet."""
        extract_from_kaggle()  
    
    @task()
    def load():
        """This function will load the parquet data into Snowflake."""
        dump_data()

    @task()
    def run_dbt_deps():
        """This function runs dbt deps to install the required dependencies."""
        try:
            profiles_dir = "/usr/local/airflow/dags/transform"
            project_dir = "/usr/local/airflow/dags/transform"

            # Optionally, modify environment variables
            env = os.environ.copy()

            result = subprocess.run(
                ['dbt', 'deps', '--profiles-dir', profiles_dir],
                cwd=project_dir,
                env=env,
                check=True,
                capture_output=True,
                text=True
            )
            print(result.stdout)
            print(result.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error: {e.stderr}")
            raise Exception(f"Error occurred while running dbt deps: {e.stderr}")
    
    # Define DbtTaskGroups inside the DAG function
    def create_dbt_task_groups():
        staging = DbtTaskGroup(
            group_id='staging',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/staging']
            )
        )
        
        intermediate = DbtTaskGroup(
            group_id='intermediate',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/intermediate']
            )
        )
        
        analytics = DbtTaskGroup(
            group_id='analytics',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=['path:models/analytics']
            )
        )
        
        return staging, intermediate, analytics

    # Call the function to create task groups
    staging, intermediate, analytics = create_dbt_task_groups()
    
    send_success_email = EmailOperator(
    task_id='send_success_email',
    to='jmelvinsarabia032@gmail.com',
    subject='E-Commerce Data Pipeline - Success Notification',
    html_content="""<h3>E-Commerce Data Pipeline completed successfully</h3>
    <p>The pipeline run for {{ ds }} has completed successfully.</p>
    <p>Execution time: {{ (execution_date + macros.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S") }}</p>"""  # Replace non-breaking spaces with regular spaces
)

    # Define the task dependencies (task flow)
    extract() >> load() >> run_dbt_deps() >> staging >> intermediate >> analytics >> send_success_email

# Instantiate the DAG
ecommerce_data_pipeline_dag = ecommerce_data_pipeline()
