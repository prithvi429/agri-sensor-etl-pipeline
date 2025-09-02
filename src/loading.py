import os
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

PROCESSED_DIR = "data/processed"

def save_partitioned_parquet(df: pd.DataFrame):
    if df.empty:
        print("[LOAD] No data to write.")
        return

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Partition by date and sensor_id
    df = df.copy()
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date.astype(str)

    for date, date_df in df.groupby("date"):
        for sensor_id, sensor_df in date_df.groupby("sensor_id"):
            dir_path = os.path.join(PROCESSED_DIR, f"date={date}", f"sensor_id={sensor_id}")
            os.makedirs(dir_path, exist_ok=True)
            file_path = os.path.join(dir_path, "data.parquet")
            table = pa.Table.from_pandas(sensor_df, preserve_index=False)
            pq.write_table(table, file_path, compression="snappy")
            print(f"[LOAD] Wrote {len(sensor_df)} rows → {file_path}")
