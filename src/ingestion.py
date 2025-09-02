import os
import duckdb
import pandas as pd
from datetime import datetime
RAW_DATA_DIR = "data/raw/"
PROCESSED_FILES_LOG = "data/processed/processed_files.txt"
def list_parquet_files():
    return sorted([f for f in os.listdir(RAW_DATA_DIR) if f.endswith(".parquet")])
def read_parquet_with_duckdb(file_path):
    try:
        con = duckdb.connect()
        # Inspect schema
        schema = con.execute(f"DESCRIBE SELECT * FROM parquet_scan('{file_path}')").fetchdf()
        # Basic validation: check required columns
        required_cols = {"sensor_id", "timestamp", "reading_type", "value", "battery_level"}
        if not required_cols.issubset(set(schema['column_name'])):
            raise ValueError(f"Schema mismatch in {file_path}")
        # Read data
        df = con.execute(f"SELECT * FROM parquet_scan('{file_path}')").fetchdf()
        return df, None
    except Exception as e:
        return None, str(e)
def load_incremental():
    processed_files = set()
