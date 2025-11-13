from src.ingestion.ingest_orders import load_raw_orders
from src.transformations.transform_orders import transform_orders
from src.transformations.data_quality import validate_orders
from src.warehouse.load_to_db import create_schema, load_fact_orders, load_dimensions
from src.config import PROCESSED_DATA_PATH
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def run_pipeline():
    """
    Run the whole pipeline:
    1. Ingest raw data
    2. Transform
    3. Data quality checks
    4. Save processed file
    5. Create schema
    6. Load dimensions
    7. Load fact table
    """
    logger.info("Step 1: Loading raw orders...")
    df_raw = load_raw_orders()
    logger.info("Loaded %d raw rows.", len(df_raw))

    logger.info("Step 2: Transforming orders...")
    df_clean = transform_orders(df_raw)
    logger.info("Transformed to %d clean rows.", len(df_clean))

    logger.info("Step 3: Running data quality checks...")
    validate_orders(df_clean)
    logger.info("Data quality checks passed.")

    logger.info("Step 4: Saving processed CSV...")
    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(PROCESSED_DATA_PATH, index=False)
    logger.info("Saved processed data to %s", PROCESSED_DATA_PATH)

    logger.info("Step 5: Creating schema...")
    create_schema()
    logger.info("Schema created (if not already present).")

    logger.info("Step 6: Loading dimension tables...")
    load_dimensions(df_clean)
    logger.info("Loaded dim_customers and dim_products.")

    logger.info("Step 7: Loading fact_orders table...")
    load_fact_orders(df_clean)
    logger.info("Loaded fact_orders.")

    logger.info("Pipeline completed successfully.")

