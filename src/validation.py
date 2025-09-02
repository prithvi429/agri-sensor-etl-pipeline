import duckdb
import pandas as pd

from ingestion import EXPECTED_RANGES

def validate_data(df: pd.DataFrame, output_path="data/data_quality_report.csv"):
    """
    Runs data quality checks in DuckDB:
    - Type checks
    - Range checks per reading_type
    - Hourly gaps per sensor
    - Basic profile: missing %, anomalous %
    Writes a single CSV report.
    """
    if df.empty:
        # Write an empty report with a message
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("No data to validate\n")
        print(f"Data quality report saved to {output_path} (no data)")
        return

    # Prepare a copy for DuckDB (strings where needed)
    work = df.copy()
    work["timestamp_str"] = work["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
    con = duckdb.connect()
    con.register("sensor_data", work)

    # Type check (duckdb typeof on columns)
    type_check = con.execute("""
        SELECT
          COUNT(*) AS total_records,
          SUM(CASE WHEN typeof(value) NOT IN ('DOUBLE','FLOAT','DECIMAL','HUGEINT','BIGINT','INTEGER','SMALLINT','TINYINT') THEN 1 ELSE 0 END) AS invalid_value_type,
          SUM(CASE WHEN typeof(battery_level) NOT IN ('DOUBLE','FLOAT','DECIMAL','HUGEINT','BIGINT','INTEGER','SMALLINT','TINYINT') THEN 1 ELSE 0 END) AS invalid_battery_type
        FROM sensor_data
    """).df()

    # Range check per reading_type using EXPECTED_RANGES
    # Build a small helper table for ranges
    ranges_rows = [(k, v[0], v[1]) for k, v in EXPECTED_RANGES.items()]
    ranges_df = pd.DataFrame(ranges_rows, columns=["reading_type", "lo", "hi"])
    con.register("ranges", ranges_df)

    range_check = con.execute("""
        SELECT
            sd.reading_type,
            COUNT(*) AS total,
            SUM(CASE WHEN sd.value < r.lo OR sd.value > r.hi THEN 1 ELSE 0 END) AS out_of_range
        FROM sensor_data sd
        LEFT JOIN ranges r
          ON sd.reading_type = r.reading_type
        GROUP BY sd.reading_type
        ORDER BY sd.reading_type
    """).df()

    # Hourly gaps per sensor between min/max time
    gaps = con.execute("""
        WITH bounds AS (
          SELECT
            sensor_id,
            MIN(timestamp_str)::TIMESTAMP AS min_ts,
            MAX(timestamp_str)::TIMESTAMP AS max_ts
          FROM sensor_data
          GROUP BY sensor_id
        ),
        expected AS (
          SELECT
            b.sensor_id,
            gs AS ts
          FROM bounds b,
          LATERAL generate_series(b.min_ts, b.max_ts, INTERVAL 1 HOUR) AS gs
        ),
        actual AS (
          SELECT sensor_id, timestamp_str::TIMESTAMP AS ts
          FROM sensor_data
          GROUP BY sensor_id, ts
        )
        SELECT
          e.sensor_id,
          COUNT(*) FILTER (WHERE a.ts IS NULL) AS missing_hours
        FROM expected e
        LEFT JOIN actual a
          ON e.sensor_id = a.sensor_id AND e.ts = a.ts
        GROUP BY e.sensor_id
        ORDER BY e.sensor_id
    """).df()

    # Profile: missing % per reading_type and anomalous %
    profile = con.execute("""
        SELECT
          reading_type,
          COUNT(*) AS total,
          SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_missing_value,
          SUM(CASE WHEN anomalous_reading THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_anomalous
        FROM sensor_data
        GROUP BY reading_type
        ORDER BY reading_type
    """).df()

    # Save a single report CSV by concatenating with section headers
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("Type Check:\n")
        type_check.to_csv(f, index=False)
        f.write("\nRange Check:\n")
        range_check.to_csv(f, index=False)
        f.write("\nGaps:\n")
        gaps.to_csv(f, index=False)
        f.write("\nProfile:\n")
        profile.to_csv(f, index=False)

    print(f"Data quality report saved to {output_path}")
