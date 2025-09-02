import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Load your existing raw file
df = pd.read_parquet("data/raw/2025-06-05.parquet")

# Function to shift timestamp and save as new file
def make_new_file(df, days_shift, out_file):
    new_df = df.copy()
    # shift timestamp
    new_df["timestamp"] = pd.to_datetime(new_df["timestamp"]) + pd.Timedelta(days=days_shift)
    # add small noise to values so files are not identical
    new_df["value"] = new_df["value"] * (1 + 0.05 * np.random.randn(len(new_df)))
    # save as parquet
    table = pa.Table.from_pandas(new_df)
    pq.write_table(table, out_file)
    print(f"✅ Created {out_file}")

# Create 2 more files for demo
make_new_file(df, 1, "data/raw/2025-06-06.parquet")
make_new_file(df, 2, "data/raw/2025-06-07.parquet")
