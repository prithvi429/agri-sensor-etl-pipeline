import pandas as pd
import numpy as np
from scipy.stats import zscore
from ingestion import CALIBRATION_PARAMS, EXPECTED_RANGES
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    # Drop duplicates
    df = df.drop_duplicates()
    # Drop rows with missing critical values
    df = df.dropna(subset=["sensor_id", "timestamp", "reading_type", "value"])
    # Detect outliers using z-score per reading_type
    def detect_outliers(group):
        group['zscore'] = zscore(group['value'])
        # Correct outliers by capping to 3 std dev
        group.loc[group['zscore'].abs() > 3, 'value'] = group.loc[group['zscore'].abs() <= 3, 'value'].mean()
        return group.drop(columns=['zscore'])
    df = df.groupby("reading_type").apply(detect_outliers).reset_index(drop=True)
    return df
def derive_features(df: pd.DataFrame) -> pd.DataFrame:
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
