import duckdb
import pandas as pd
from ingestion import EXPECTED_RANGES

def validate_data(df: pd.DataFrame, output_path="data/data_quality_report.csv", fail_on_error=True):
    """
    Runs data quality checks in DuckDB:
    - Type checks
    - Range checks per reading_type
    - Hourly gaps per sensor
    - Basic profile: missing %, anomalous %
    Writes a structured CSV report.
    Optionally stops pipeline if critical failures.
    """
    if df.empty:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("No data to validate\n")
        print(f"⚠️ No data to validate, report saved at {output_path}")
        return

    work = df.copy()
    work["timestamp_str"] = (
        work["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    con = duckdb.connect()
    con.register("sensor_data", work)

    # ---------- TYPE CHECK ----------
    type_check = con.execute("""
        SELECT
          COUNT(*) AS total_records,
          SUM(CASE WHEN typeof(value) NOT IN 
              ('DOUBLE','FLOAT','DECIMAL','HUGEINT','BIGINT','INTEGER','SMALLINT','TINYINT') 
              THEN 1 ELSE 0 END) AS invalid_value_type,
          SUM(CASE WHEN typeof(battery_level) NOT IN 
              ('DOUBLE','FLOAT','DECIMAL','HUGEINT','BIGINT','INTEGER','SMALLINT','TINYINT') 
              THEN 1 ELSE 0 END) AS invalid_battery_type
        FROM sensor_data
    """).df()

    # ---------- RANGE CHECK ----------
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

    # ---------- HOURLY GAPS ----------
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
                gs.generate_series AS ts
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
    """).df()

    # ---------- PROFILE ----------
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

    # ---------- SAVE CONSOLIDATED REPORT ----------
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("section,details\n")
        type_check.to_csv(f, index=False)
        range_check.to_csv(f, index=False)
        gaps.to_csv(f, index=False)
        profile.to_csv(f, index=False)

    print(f"✅ Data quality report saved to {output_path}")

    # ---------- FAIL FAST (optional) ----------
    if fail_on_error:
        if int(type_check["invalid_value_type"][0]) > 0 or int(type_check["invalid_battery_type"][0]) > 0:
            raise ValueError("❌ Validation failed: Invalid data types detected")
        if (profile["pct_missing_value"] > 10).any():
            raise ValueError("❌ Validation failed: Too many missing values")
