import pandas as pd

from src.warehouse.db import get_engine
from src.config import REPORTS_DIR
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def top_products_by_revenue(limit: int = 10, engine=None) -> pd.DataFrame:
    """
    Return top products by total revenue.
    """
    if engine is None:
        engine = get_engine()

    query = f"""
    SELECT
        p.product_name,
        p.category,
        SUM(f.total_amount) AS revenue
    FROM fact_orders f
    JOIN dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_name, p.category
    ORDER BY revenue DESC
    LIMIT {int(limit)}
    """
    return pd.read_sql_query(query, engine)


def revenue_by_month(engine=None) -> pd.DataFrame:
    """
    Return monthly revenue.
    """
    if engine is None:
        engine = get_engine()

    query = """
    SELECT
        substr(order_date, 1, 7) AS year_month,
        SUM(total_amount) AS revenue
    FROM fact_orders
    GROUP BY year_month
    ORDER BY year_month;
    """
    return pd.read_sql_query(query, engine)


def revenue_by_country(engine=None) -> pd.DataFrame:
    """
    Return revenue per customer country.
    """
    if engine is None:
        engine = get_engine()

    query = """
    SELECT
        c.country,
        SUM(f.total_amount) AS revenue
    FROM fact_orders f
    JOIN dim_customers c ON f.customer_id = c.customer_id
    GROUP BY c.country
    ORDER BY revenue DESC;
    """
    return pd.read_sql_query(query, engine)


def generate_all_reports(limit: int = 10) -> None:
    """
    Generate CSV reports under the reports/ directory:
    - top_products_by_revenue.csv
    - revenue_by_month.csv
    - revenue_by_country.csv
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Generating analytics reports into %s", REPORTS_DIR)

    engine = get_engine()

    df_top_products = top_products_by_revenue(limit=limit, engine=engine)
    top_products_path = REPORTS_DIR / "top_products_by_revenue.csv"
    df_top_products.to_csv(top_products_path, index=False)
    logger.info("Wrote %s", top_products_path)

    df_monthly = revenue_by_month(engine=engine)
    monthly_path = REPORTS_DIR / "revenue_by_month.csv"
    df_monthly.to_csv(monthly_path, index=False)
    logger.info("Wrote %s", monthly_path)

    df_country = revenue_by_country(engine=engine)
    country_path = REPORTS_DIR / "revenue_by_country.csv"
    df_country.to_csv(country_path, index=False)
    logger.info("Wrote %s", country_path)
