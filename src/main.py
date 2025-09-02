from ingestion import load_incremental
from transformation import clean_data, derive_features
from validation import validate_data
from loading import save_partitioned_parquet

def run_pipeline():
    print("Starting ingestion...")
    raw_df, stats = load_incremental()
    print(f"Ingestion stats: {stats}")
    if raw_df.empty:
        print("No new data to process.")
        return

    print("Starting transformation...")
    cleaned_df = clean_data(raw_df)
    enriched_df = derive_features(cleaned_df)

    print("Starting validation...")
    validate_data(enriched_df)

    print("Saving processed data...")
    save_partitioned_parquet(enriched_df)

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
