import os
from datetime import datetime
import duckdb
import pandas as pd

RAW_DATA_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
PROCESSED_FILES_LOG = os.path.join(PROCESSED_DIR, "processed_files.txt")

# Expected ranges per reading_type (you can tune these)
EXPECTED_RANGES = {
    "temperature": (-20.0, 60.0),
    "humidity": (0.0, 100.0),
    "soil_moisture": (0.0, 100.0),
    "light": (0.0, 200000.0),
    "battery_level": (0.0, 100.0),
}

# Calibration parameters per reading_type: value = raw * multiplier + offset
CALIBRATION_PARAMS = {
    "temperature": {"multiplier": 1.0, "offset": 0.0},
    "humidity": {"multiplier": 1.0, "offset": 0.0},
    "soil_moisture": {"multiplier": 1.0, "offset": 0.0},
    "light": {"multiplier": 1.0, "offset": 0.0},
}

REQUIRED_COLS = {"sensor_id", "timestamp", "reading_type", "value", "battery_level"}


def _ensure_dirs():
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)


def _read_processed_log() -> set:
    _ensure_dirs()
    if not os.path.exists(PROCESSED_FILES_LOG):
        return set()
    with open(PROCESSED_FILES_LOG, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def _append_processed_log(filename: str):
    with open(PROCESSED_FILES_LOG, "a", encoding="utf-8") as f:
        f.write(filename + "\n")


def _list_parquet_files() -> list:
    _ensure_dirs()
    return sorted([f for f in os.listdir(RAW_DATA_DIR) if f.lower().endswith(".parquet")])


def _read_parquet_duckdb(file_path: str) -> pd.DataFrame:
    con = duckdb.connect()
    # Validate schema
    schema = con.execute(
        f"DESCRIBE SELECT * FROM parquet_scan('{file_path}')"
    ).fetchdf()
    cols = set(schema["column_name"].tolist())
    if not REQUIRED_COLS.issubset(cols):
        raise ValueError(f"Schema mismatch in {file_path}. Required: {REQUIRED_COLS}, found: {cols}")

    # Read
    df = con.execute(f"SELECT * FROM parquet_scan('{file_path}')").fetchdf()
    return df


def load_incremental():
    """
    Reads all new parquet files from data/raw (those not listed in processed_files.txt),
    validates schemas, concatenates into a single DataFrame, and updates the log.
    Returns (df, stats_dict).
    """
    _ensure_dirs()
    processed = _read_processed_log()
    files = _list_parquet_files()

    new_files = [f for f in files if f not in processed]
    stats = {
        "total_files": len(files),
        "new_files": len(new_files),
        "processed_now": 0,
        "rows_loaded": 0,
        "failed_files": [],
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    dfs = []
    for fname in new_files:
        fpath = os.path.join(RAW_DATA_DIR, fname)
        try:
            df = _read_parquet_duckdb(fpath)
            dfs.append(df)
            _append_processed_log(fname)
            stats["processed_now"] += 1
            stats["rows_loaded"] += len(df)
        except Exception as e:
            stats["failed_files"].append({"file": fname, "error": str(e)})

    if dfs:
        full_df = pd.concat(dfs, ignore_index=True)
    else:
        full_df = pd.DataFrame(columns=list(REQUIRED_COLS))

    return full_df, stats
