import pandas as pd
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

def validate_orders(df: pd.DataFrame) -> None:
    """
    Run basic data quality checks on the orders DataFrame.

    - No null order_id
    - No duplicate order_id
    - quantity > 0
    - unit_price > 0

    Raises:
        ValueError if any check fails.
    """
    errors = []

    if df["order_id"].isna().any():
        errors.append("order_id contains null values")

    dup_count = df["order_id"].duplicated().sum()
    if dup_count > 0:
        errors.append(f"{dup_count} duplicate order_id values found")

    bad_qty = (df["quantity"] <= 0).sum()
    if bad_qty > 0:
        errors.append(f"{bad_qty} rows with non-positive quantity")

    bad_price = (df["unit_price"] <= 0).sum()
    if bad_price > 0:
        errors.append(f"{bad_price} rows with non-positive unit_price")

    logger.info(
        "Data quality summary: rows=%d, columns=%d, issues=%d",
        len(df),
        df.shape[1],
        len(errors),
    )

    if errors:
        for e in errors:
            logger.error("Data quality issue: %s", e)
        raise ValueError("Data quality validation failed; see logs for details")
