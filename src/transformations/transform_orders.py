import pandas as pd

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning: drop duplicates and rows missing critical fields.
    """
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "customer_id", "product_id", "order_date"])
    return df

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns like total_amount, order_year, order_month.
    """
    df["total_amount"] = df["quantity"] * df["unit_price"]

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_year"] = df["order_date"].dt.year
    df["order_month"] = df["order_date"].dt.month

    return df

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_orders(df)
    df = add_features(df)
    return df
