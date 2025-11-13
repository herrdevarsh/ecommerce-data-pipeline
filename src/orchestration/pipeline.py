from sqlalchemy import text

from src.ingestion.ingest_orders import load_raw_orders
from src.transformations.transform_orders import transform_orders
from src.transformations.data_quality import validate_orders
from src.warehouse.load_to_db import create_schema, load_fact_orders, load_dimensions
from src.warehouse.db import get_engine
from src.config import PROCESSED_DATA_PATH
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def log_table_row_counts():
    """
    Log row counts for key tables in the warehouse.
    """
    engine = get_engine()
    tables = ["dim_customers", "dim_products", "fact_orders"]

    for table in tables:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar_one()
            logger.info("Table %s row count: %s", table, count)
        except Exception as exc:
            logger.warning("Could not read row count for %s: %s", table, exc)


def run_pipeline(dry_run: bool = False):
    """
    Run the whole pipeline:
    1. Ingest raw data
    2. Transform
    3. Data quality checks
    4. (optional) Save processed file
    5. (optional) Create schema
    6. (optional) Load dimensions
    7. (optional) Load fact table
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

    if dry_run:
        logger.info(
            "Dry-run mode: skipping file save and database load. Pipeline stops after validation."
        )
        return

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

    log_table_row_counts()

    logger.info("Pipeline completed successfully.")
