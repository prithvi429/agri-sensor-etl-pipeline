import duckdb
import pandas as pd

def validate_data(df: pd.DataFrame, output_path="data_quality_report.csv"):
    con = duckdb.connect()

    # Load dataframe into DuckDB
    con.register('sensor_data', df)

    # Validate types
    type_check = con.execute("""
        SELECT
            COUNT(*) AS total_records,
            SUM(CASE WHEN typeof(value) != 'FLOAT' THEN 1 ELSE 0 END) AS invalid_value_type,
            SUM(CASE WHEN typeof(timestamp) != 'VARCHAR' THEN 1 ELSE 0 END) AS invalid_timestamp_type
        FROM sensor_data
    """).fetchdf()

    # Check expected value ranges per reading_type
    range_check = con.execute("""
        SELECT reading_type,
            COUNT(*) AS total,
            SUM(CASE WHEN value < low OR value > high THEN 1 ELSE 0 END) AS out_of_range
        FROM sensor_data
        JOIN (
            VALUES
                ('temperature', -10, 50),
                ('humidity', 0, 100),
                ('soil_moisture', 0, 100),
                ('light_intensity', 0, 2000),
                ('battery_level', 0, 100)
        ) AS ranges(reading_type, low, high)
        USING (reading_type)
        GROUP BY reading_type
    """).fetchdf()

    # Detect gaps in hourly data per sensor_id and reading_type
    # Generate expected hourly timestamps for the date range
    min_ts = df['timestamp'].min()
    max_ts = df['timestamp'].max()

    # Convert timestamps to datetime for generate_series
    con.execute(f"""
        CREATE TABLE sensor_data_ts AS
        SELECT sensor_id, reading_type, CAST(timestamp AS TIMESTAMP) AS ts
        FROM sensor_data
    """)

    gap_query = f"""
        WITH sensors AS (
            SELECT DISTINCT sensor_id, reading_type FROM sensor_data_ts
        ),
        hours AS (
            SELECT generate_series(
                (SELECT MIN(ts) FROM sensor_data_ts),
                (SELECT MAX(ts) FROM sensor_data_ts),
                INTERVAL 1 hour
            ) AS hour_ts
        ),
        expected AS (
            SELECT sensor_id, reading_type, hour_ts
            FROM sensors CROSS JOIN hours
        ),
        missing AS (
            SELECT expected.sensor_id, expected.reading_type, expected.hour_ts
            FROM expected
            LEFT JOIN sensor_data_ts s
            ON expected.sensor_id = s.sensor_id
            AND expected.reading_type = s.reading_type
            AND expected.hour_ts = date_trunc('hour', s.ts)
            WHERE s.ts IS NULL
        )
        SELECT sensor_id, reading_type, COUNT(*) AS missing_hours
        FROM missing
        GROUP BY sensor_id, reading_type
    """

    gaps = con.execute(gap_query).fetchdf()

    # Profile missing and anomalous values
    profile = con.execute("""
        SELECT reading_type,
            100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS pct_missing,
            100.0 * SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) / COUNT(*) AS pct_anomalous
        FROM sensor_data
        GROUP BY reading_type
    """).fetchdf()

    # Save report
    with open(output_path, "w") as f:
        f.write("Type Check:\n")
        type_check.to_csv(f, index=False)
        f.write("\nRange Check:\n")
        range_check.to_csv(f, index=False)
        f.write("\nGaps:\n")
        gaps.to_csv(f, index=False)
        f.write("\nProfile:\n")
        profile.to_csv(f, index=False)

    print(f"Data quality report saved to {output_path}")