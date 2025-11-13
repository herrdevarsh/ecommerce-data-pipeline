from sqlalchemy import text
from src.warehouse.db import get_engine

def create_schema():
    """
    Create dim_customers, dim_products, and fact_orders tables if they do not exist.
    """
    engine = get_engine()
    with engine.begin() as conn:
        # Dimension table: customers
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            country TEXT
        );
        """))

        # Dimension table: products
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT,
            category TEXT
        );
        """))

        # Fact table: orders
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            order_date TEXT,
            quantity INTEGER,
            unit_price REAL,
            total_amount REAL,
            FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
            FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
        );
        """))

def load_dimensions(df):
    """
    Build and load dim_customers and dim_products from the transformed DataFrame.
    """
    from src.warehouse.db import get_engine  # avoid circular import
    engine = get_engine()

    # Build customer dimension
    dim_customers = df[["customer_id", "customer_name", "country"]].drop_duplicates().copy()

    # Build product dimension
    dim_products = df[["product_id", "product_name", "category"]].drop_duplicates().copy()

    # Load into database (replace each time)
    dim_customers.to_sql("dim_customers", engine, if_exists="replace", index=False)
    dim_products.to_sql("dim_products", engine, if_exists="replace", index=False)

def load_fact_orders(df):
    """
    Load transformed orders DataFrame into the fact_orders table.
    """
    from src.warehouse.db import get_engine
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
