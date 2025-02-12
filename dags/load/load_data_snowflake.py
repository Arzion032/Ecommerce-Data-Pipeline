import snowflake.connector
import shutil
import os
from dotenv import load_dotenv
from pathlib import Path

def dump_data():
    env_path = Path(__file__).resolve().parent.parent.parent/'.env'

    load_dotenv(dotenv_path=env_path)

    schema_name = os.getenv('SNOWFLAKE_SCHEMA')
    role = os.getenv('SNOWFLAKE_ROLE')
    stage_creation_query = f"""
    CREATE STAGE IF NOT EXISTS {schema_name}.product_stage
    FILE_FORMAT=(TYPE=PARQUET);
    """

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=schema_name
    )

    dataset_path = Path(__file__).resolve().parent.parent/'extract'/'datasets'

    cur = conn.cursor()
    cur.execute(f"USE ROLE {role}")
    cur.execute(stage_creation_query)

    # Upload file to Snowflake stage
    for file in dataset_path.glob("*.parquet"):
        # Convert to absolute path and replace backslashes with forward slashes
        file_path = str(file.absolute()).replace('\\', '/')
    
        file_path = f"'file://{file_path}'"
            
        # Create the PUT command with proper syntax
        put_command = f"PUT {file_path} @product_stage OVERWRITE = TRUE"
        cur.execute(put_command)

    # Confirm upload
    cur.execute("LIST @product_stage")
    print(cur.fetchall())

    cur.close()
    
    shutil.rmtree(dataset_path)