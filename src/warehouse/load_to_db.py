from sqlalchemy import text
from src.warehouse.db import get_engine

def create_schema():
    """
    Create tables in the SQLite database if they do not exist.
    Here we only use fact_orders for simplicity.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            order_date TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL
        );
        """))

def load_fact_orders(df):
    """
    Load transformed orders DataFrame into the fact_orders table.
    This will REPLACE the table each time.
    """
    from src.warehouse.db import get_engine  # local import to avoid cycles
    engine = get_engine()

    df_to_load = df[[
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "total_amount"
    ]].copy()

    df_to_load.to_sql(
        "fact_orders",
        engine,
        if_exists="replace",
        index=False
    )
