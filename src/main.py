from pathlib import Path
from ingestion import load_incremental
from transformation import clean_data, derive_features
from validation import validate_data
from loading import save_partitioned_parquet
from generate_sample_data import generate_demo_files


def run_pipeline():
    # Ensure we have at least 3 raw files before ingestion
    raw_dir = Path("data/raw")
    raw_files = list(raw_dir.glob("*.parquet"))
    if len(raw_files) < 3:
        print("⚙️ Not enough raw files found. Generating demo data...")
        generate_demo_files()
        raw_files = list(raw_dir.glob("*.parquet"))

    print("📥 Starting ingestion...")
    raw_df, stats = load_incremental()
    print(f"✅ Ingestion stats: {stats}")

    if raw_df.empty:
        print("⚠️ No new data to process. Exiting pipeline.")
        return

    print("🧹 Starting transformation...")
    cleaned_df = clean_data(raw_df)
    enriched_df = derive_features(cleaned_df)

    print("🔍 Starting validation...")
    validate_data(enriched_df)

    print("💾 Saving processed data...")
    save_partitioned_parquet(enriched_df)

    print("🎉 Pipeline completed successfully!")


if __name__ == "__main__":
    run_pipeline()
