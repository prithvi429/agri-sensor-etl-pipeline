import pandas as pd
import numpy as np
from pathlib import Path

def generate_demo_files():
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    dates = ["2025-06-05", "2025-06-06", "2025-06-07"]
    sensors = [f"sensor_{i}" for i in range(1, 6)]

    for d in dates:
        rows = []
        for s in sensors:
            for i in range(10):  # 10 rows per sensor
                rows.append({
                    "sensor_id": s,
                    "timestamp": f"{d}T12:{i:02d}:00Z",
                    "reading_type": np.random.choice(["temperature", "humidity", "soil_moisture", "light", "battery"]),
                    "value": float(np.random.uniform(10, 100)),
                    "battery_level": float(np.random.uniform(20, 100))
                })
        df = pd.DataFrame(rows)
        df.to_parquet(f"data/raw/{d}.parquet")
    print("✅ Demo raw files generated: 2025-06-05, 2025-06-06, 2025-06-07")
