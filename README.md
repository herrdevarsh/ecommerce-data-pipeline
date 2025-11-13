# E-commerce Data Pipeline

## Overview

This project is a simple end-to-end batch data pipeline:

- Ingests raw e-commerce orders from a CSV file
- Cleans and transforms the data with Python (pandas)
- Loads the data into a SQLite database (data warehouse)
- Runs SQL analytics (top products, monthly revenue, etc.)
- Includes basic tests for transformation logic

## Architecture

- **Ingestion**: read `data/raw/orders_raw.csv` with pandas
- **Transformations**: clean data, remove duplicates, create `total_amount`
- **Warehouse**: SQLite database file `warehouse.db`
- **Orchestration**: `src/main.py` calls the full pipeline
- **Testing**: pytest in `tests/`

## Data Model (simplified)

- `fact_orders(order_id, customer_id, product_id, order_date, quantity, unit_price, total_amount)`

## How to run

```bash
python -m venv venv
# Windows: venv\Scripts\activate
# Mac/Linux: source venv/bin/activate

pip install -r requirements.txt

# Run the pipeline
python -m src.main

# Run tests
pytest
