from src.ingestion.ingest_orders import load_raw_orders
from src.transformations.transform_orders import transform_orders
from src.warehouse.load_to_db import create_schema, load_fact_orders
from src.config import PROCESSED_DATA_PATH

def run_pipeline():
    """
    Run the whole pipeline:
    1. Ingest raw data
    2. Transform
    3. Save processed file
    4. Load into SQLite database
    """
    print("Step 1: Loading raw orders...")
    df_raw = load_raw_orders()
    print(f"Loaded {len(df_raw)} raw rows.")

    print("Step 2: Transforming orders...")
    df_clean = transform_orders(df_raw)
    print(f"Transformed to {len(df_clean)} clean rows.")

    print("Step 3: Saving processed CSV...")
    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(PROCESSED_DATA_PATH, index=False)
    print(f"Saved processed data to {PROCESSED_DATA_PATH}")

    print("Step 4: Creating schema and loading into database...")
    create_schema()
    load_fact_orders(df_clean)
    print("Loaded data into SQLite database.")

    print("Pipeline completed successfully.")
