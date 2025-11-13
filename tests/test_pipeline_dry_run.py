import sys
from pathlib import Path

import pytest

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.orchestration.pipeline import run_pipeline  # noqa: E402


def test_run_pipeline_dry_run_skips_db(monkeypatch):
    calls = {
        "create_schema": 0,
        "load_dimensions": 0,
        "load_fact_orders": 0,
    }

    def fake_create_schema():
        calls["create_schema"] += 1

    def fake_load_dimensions(df):
        calls["load_dimensions"] += 1

    def fake_load_fact_orders(df):
        calls["load_fact_orders"] += 1

    # Patch the functions inside the pipeline module
    monkeypatch.setattr("src.orchestration.pipeline.create_schema", fake_create_schema)
    monkeypatch.setattr(
        "src.orchestration.pipeline.load_dimensions", fake_load_dimensions
    )
    monkeypatch.setattr(
        "src.orchestration.pipeline.load_fact_orders", fake_load_fact_orders
    )

    # This should not call any of the patched DB functions
    run_pipeline(dry_run=True)

    assert calls["create_schema"] == 0
    assert calls["load_dimensions"] == 0
    assert calls["load_fact_orders"] == 0
