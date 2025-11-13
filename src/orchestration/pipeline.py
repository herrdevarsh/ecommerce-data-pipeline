import json
from datetime import datetime

from sqlalchemy import text

from src.ingestion.ingest_orders import load_raw_orders
from src.transformations.transform_orders import transform_orders
from src.transformations.data_quality import validate_orders
from src.warehouse.load_to_db import create_schema, load_fact_orders, load_dimensions
from src.warehouse.db import get_engine
from src.config import PROCESSED_DATA_PATH, LOGS_DIR
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def get_table_row_counts():
    """
    Return and log row counts for key tables in the warehouse.
    """
    engine = get_engine()
    tables = ["dim_customers", "dim_products", "fact_orders"]
    counts = {}

    for table in tables:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar_one()
            counts[table] = count
            logger.info("Table %s row count: %s", table, count)
        except Exception as exc:
            counts[table] = None
            logger.warning("Could not read row count for %s: %s", table, exc)

    return counts


def write_run_summary(
    *,
    started_at: datetime,
    dry_run: bool,
    raw_rows: int,
    clean_rows: int,
    table_counts: dict | None,
    status: str,
    error_message: str | None = None,
) -> None:
    """
    Write a JSON summary of the last pipeline run.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = LOGS_DIR / "run_summary.json"

    summary = {
        "started_at_utc": started_at.isoformat() + "Z",
        "dry_run": dry_run,
        "status": status,
        "error_message": error_message,
        "raw_rows": raw_rows,
        "clean_rows": clean_rows,
        "table_counts": table_counts or {},
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logger.info("Wrote run summary to %s", summary_path)


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
    started_at = datetime.utcnow()
    raw_rows = 0
    clean_rows = 0
    table_counts = None

    try:
        logger.info("Step 1: Loading raw orders...")
        df_raw = load_raw_orders()
        raw_rows = len(df_raw)
        logger.info("Loaded %d raw rows.", raw_rows)

        logger.info("Step 2: Transforming orders...")
        df_clean = transform_orders(df_raw)
        clean_rows = len(df_clean)
        logger.info("Transformed to %d clean rows.", clean_rows)

        logger.info("Step 3: Running data quality checks...")
        validate_orders(df_clean)
        logger.info("Data quality checks passed.")

        if dry_run:
            logger.info(
                "Dry-run mode: skipping file save and database load. Pipeline stops after validation."
            )
            write_run_summary(
                started_at=started_at,
                dry_run=True,
                raw_rows=raw_rows,
                clean_rows=clean_rows,
                table_counts=None,
                status="success",
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

        table_counts = get_table_row_counts()

        write_run_summary(
            started_at=started_at,
            dry_run=False,
            raw_rows=raw_rows,
            clean_rows=clean_rows,
            table_counts=table_counts,
            status="success",
        )

        logger.info("Pipeline completed successfully.")

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        write_run_summary(
            started_at=started_at,
            dry_run=dry_run,
            raw_rows=raw_rows,
            clean_rows=clean_rows,
            table_counts=table_counts,
            status="failed",
            error_message=str(exc),
        )
        raise
