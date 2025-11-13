import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import pytest
from src.transformations.data_quality import validate_orders

def test_validate_orders_passes_on_good_data():
    df = pd.DataFrame([
        {
            "order_id": 1,
            "customer_id": 10,
            "product_id": 100,
            "order_date": "2024-01-01",
            "quantity": 2,
            "unit_price": 5.0,
            "total_amount": 10.0,
        }
    ])

    # Should NOT raise
    validate_orders(df)

def test_validate_orders_fails_on_negative_quantity():
    df = pd.DataFrame([
        {
            "order_id": 1,
            "customer_id": 10,
            "product_id": 100,
            "order_date": "2024-01-01",
            "quantity": -1,
            "unit_price": 5.0,
            "total_amount": -5.0,
        }
    ])

    with pytest.raises(ValueError):
        validate_orders(df)
