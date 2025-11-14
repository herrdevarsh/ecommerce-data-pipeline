import sys
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.reports import (  # noqa: E402
    top_products_by_revenue,
    revenue_by_month,
    revenue_by_country,
)


@pytest.fixture
def temp_engine():
    """
    Create an in-memory SQLite engine with minimal schema and data
    for analytics tests.
    """
    engine = create_engine("sqlite:///:memory:", future=True)

    with engine.begin() as conn:
        # Create tables
        conn.execute(
            text(
                """
                CREATE TABLE dim_products (
                    product_id INTEGER PRIMARY KEY,
                    product_name TEXT,
                    category TEXT
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE dim_customers (
                    customer_id INTEGER PRIMARY KEY,
                    customer_name TEXT,
                    country TEXT
                );
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE fact_orders (
                    order_id INTEGER PRIMARY KEY,
                    customer_id INTEGER,
                    product_id INTEGER,
                    order_date TEXT,
                    quantity INTEGER,
                    unit_price REAL,
                    total_amount REAL
                );
                """
            )
        )

        # Seed dimension data
        conn.execute(
            text(
                """
                INSERT INTO dim_products (product_id, product_name, category)
                VALUES
                (100, 'USB Cable', 'Electronics'),
                (101, 'Wireless Mouse', 'Electronics');
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO dim_customers (customer_id, customer_name, country)
                VALUES
                (10, 'Alice', 'Germany'),
                (11, 'Bob', 'France');
                """
            )
        )

        # Seed fact data
        conn.execute(
            text(
                """
                INSERT INTO fact_orders
                    (order_id, customer_id, product_id, order_date,
                     quantity, unit_price, total_amount)
                VALUES
                (1, 10, 100, '2024-01-01', 2, 5.0, 10.0),
                (2, 11, 101, '2024-01-15', 1, 15.0, 15.0),
                (3, 10, 100, '2024-02-01', 3, 5.0, 15.0);
                """
            )
        )

    return engine


def test_top_products_by_revenue(temp_engine):
    df = top_products_by_revenue(limit=5, engine=temp_engine)
    # We know product 100 has revenue 25, product 101 has 15
    assert not df.empty
    assert "product_name" in df.columns
    assert df.iloc[0]["product_name"] == "USB Cable"
    assert df.iloc[0]["revenue"] == 25.0


def test_revenue_by_month(temp_engine):
    df = revenue_by_month(engine=temp_engine)
    # There should be two months: 2024-01 (10+15) and 2024-02 (15)
    assert set(df["year_month"]) == {"2024-01", "2024-02"}
    jan_rev = float(df.loc[df["year_month"] == "2024-01", "revenue"].iloc[0])
    feb_rev = float(df.loc[df["year_month"] == "2024-02", "revenue"].iloc[0])
    assert jan_rev == 25.0
    assert feb_rev == 15.0


def test_revenue_by_country(temp_engine):
    df = revenue_by_country(engine=temp_engine)
    # Alice (Germany) has 10 + 15 = 25; Bob (France) has 15
    assert set(df["country"]) == {"Germany", "France"}
    germany_rev = float(df.loc[df["country"] == "Germany", "revenue"].iloc[0])
    france_rev = float(df.loc[df["country"] == "France", "revenue"].iloc[0])
    assert germany_rev == 25.0
    assert france_rev == 15.0
