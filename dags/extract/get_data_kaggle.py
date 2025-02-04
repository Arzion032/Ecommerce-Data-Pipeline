import pandas as pd
import kaggle
from pathlib import Path

def extract_from_kaggle():
    # Set directory to dump the datasets from kaggle
    dataset_directory = Path(__file__).parent / 'datasets'
    
    dataset_directory.mkdir(parents=True, exist_ok=True)
    
    # Download dataset from Kaggle
    kaggle.api.dataset_download_files("oleksiimartusiuk/e-commerce-data-shein", path=dataset_directory, unzip=True)

    # Load datasets into pandas DataFrames
    csv_files = list(dataset_directory.glob("*.csv"))

    for file in csv_files:
        
        try:
            
            df = pd.read_csv(file)
            
            # Convert DataFrame to Parquet format and save it
            df.to_parquet(file.with_suffix('.parquet'), engine='pyarrow') 
            
            # Delete the original CSV file after conversion
            file.unlink()
            
            print(f"Successfully Converted {file} to {file.with_suffix('.parquet')}")
            
        except Exception as e:
            print(f"Error processing {file}: {e}")

    
    

