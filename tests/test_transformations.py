import pandas as pd
from src.transformations.transform_orders import transform_orders

def test_transform_orders_creates_total_amount():
    df = pd.DataFrame([
        {
            "order_id": 1,
            "customer_id": 10,
            "product_id": 100,
            "order_date": "2024-01-01",
            "quantity": 2,
            "unit_price": 5.0,
        }
    ])

    result = transform_orders(df)

    assert "total_amount" in result.columns
    assert result.loc[0, "total_amount"] == 10.0

def test_transform_orders_drops_duplicates():
    df = pd.DataFrame([
        {
            "order_id": 1,
            "customer_id": 10,
            "product_id": 100,
            "order_date": "2024-01-01",
            "quantity": 2,
            "unit_price": 5.0,
        },
        {
            "order_id": 1,  # duplicate
            "customer_id": 10,
            "product_id": 100,
            "order_date": "2024-01-01",
            "quantity": 3,
            "unit_price": 5.0,
        },
    ])

    result = transform_orders(df)

    assert len(result) == 1
