import pandas as pd
import numpy as np
from scipy.stats import zscore
from ingestion import CALIBRATION_PARAMS, EXPECTED_RANGES

IST_TZ = "Asia/Kolkata"  # UTC+5:30

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Drop duplicates and rows with missing criticals
    df = df.drop_duplicates()
    df = df.dropna(subset=["sensor_id", "timestamp", "reading_type", "value"])

    # Convert timestamp to pandas datetime (assume incoming is UTC or ISO)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp"])

    # Detect/correct outliers using z-score per reading_type
    def _cap_outliers(g: pd.DataFrame) -> pd.DataFrame:
        if g["value"].std(ddof=0) == 0 or len(g) < 3:
            return g
        zs = zscore(g["value"].astype(float), nan_policy="omit")
        g["zscore"] = zs
        within = g["zscore"].abs() <= 3
        if within.any():
            capped_mean = g.loc[within, "value"].mean()
            g.loc[~within, "value"] = capped_mean
        return g.drop(columns=["zscore"])

    df = df.groupby("reading_type", group_keys=False).apply(_cap_outliers)

    # Basic type coercions
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["battery_level"] = pd.to_numeric(df["battery_level"], errors="coerce")
    df = df.dropna(subset=["value"])

    return df


def derive_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Convert to IST and ISO 8601
    df["timestamp"] = df["timestamp"].dt.tz_convert(IST_TZ)
    df["timestamp_iso"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    # Date for partitioning and aggregations
    df["date"] = df["timestamp"].dt.date

    # Daily average per sensor+reading_type
    daily_avg = (
        df.groupby(["sensor_id", "reading_type", "date"], as_index=False)["value"]
        .mean()
        .rename(columns={"value": "daily_avg_value"})
    )
    df = df.merge(daily_avg, on=["sensor_id", "reading_type", "date"], how="left")

    # 7-day rolling average per sensor+reading_type based on date
    df_sorted = df.sort_values(["sensor_id", "reading_type", "date"])
    df["rolling_7d_avg"] = (
        df_sorted
        .groupby(["sensor_id", "reading_type"])["daily_avg_value"]
        .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
    )

    # Anomaly flag using EXPECTED_RANGES
    def _anomaly(row):
        rt = row["reading_type"]
        v = row["value"]
        if rt in EXPECTED_RANGES:
            lo, hi = EXPECTED_RANGES[rt]
            return not (lo <= v <= hi)
        return False

    df["anomalous_reading"] = df.apply(_anomaly, axis=1)

    # Calibration: per reading_type
    def _calibrate(row):
        rt = row["reading_type"]
        v = row["value"]
        params = CALIBRATION_PARAMS.get(rt, {"multiplier": 1.0, "offset": 0.0})
        return v * params["multiplier"] + params["offset"]

    df["calibrated_value"] = df.apply(_calibrate, axis=1)

    return df
