# E-commerce Data Pipeline – Design Document

## 1. Problem & Goals

### Problem

We want a simple but realistic **batch data pipeline** for an e-commerce system:

- Raw order data arrives as CSV files.
- Analysts and stakeholders need:
  - Cleaned, validated data
  - A basic dimensional model (facts + dimensions)
  - Reliable numbers for revenue and customer/product analysis.

This project simulates that pipeline end-to-end in a local environment.

### Goals

- Show an **end-to-end pipeline**: ingest → transform → validate → load → query.
- Use a **star-schema style model**: `fact_orders`, `dim_customers`, `dim_products`.
- Demonstrate **good engineering practices**:
  - Config/paths centralized
  - Logging and data quality checks
  - Tests and CI
  - CLI with safe `--dry-run` mode

Non-goals (for this version):

- Real-time / streaming processing
- Huge-scale distributed compute (Spark, Flink)
- Full Airflow deployment

---

## 2. High-Level Architecture

Components:

1. **Ingestion**
   - Reads `data/raw/orders_raw.csv` using pandas.
   - Implemented in `src/ingestion/ingest_orders.py`.

2. **Transformations**
   - Cleans data (deduplicates, handles nulls).
   - Adds derived fields like `total_amount`, `order_year`, `order_month`.
   - Implemented in `src/transformations/transform_orders.py`.

3. **Data Quality**
   - Validates core assumptions:
     - No null `order_id`
     - No duplicate `order_id`
     - `quantity > 0`
     - `unit_price > 0`
   - Fails the pipeline early if rules are violated.
   - Implemented in `src/transformations/data_quality.py`.

4. **Warehouse / Storage**
   - Local SQLite database: `warehouse.db`.
   - Tables:
     - `dim_customers`
     - `dim_products`
     - `fact_orders`
   - Schema + loading in `src/warehouse/load_to_db.py`.

5. **Orchestration**
   - Coordinates the steps above.
   - Supports `dry_run` mode (no file/DB writes).
   - Writes JSON run summary with row counts.
   - Implemented in `src/orchestration/pipeline.py`.

6. **Interface / CLI**
   - Single entrypoint with arguments:
     - `python -m src.main` (full run)
     - `python -m src.main --dry-run` (no DB writes)
   - Implemented in `src/main.py`.

7. **Observability & Feedback**
   - Structured logging to console and file (`logs/pipeline.log`).
   - Run summaries: `logs/run_summary.json`.

---

## 3. Data Model

The pipeline loads data into a simple star schema.

### 3.1 Dimension Tables

**dim_customers**

- `customer_id` (PK)
- `customer_name`
- `country`

Built by taking distinct combinations of customer attributes from the transformed DataFrame.

**dim_products**

- `product_id` (PK)
- `product_name`
- `category`

Built similarly from product attributes.

### 3.2 Fact Table

**fact_orders**

- `order_id` (PK)
- `customer_id` (FK → dim_customers)
- `product_id` (FK → dim_products)
- `order_date`
- `quantity`
- `unit_price`
- `total_amount` (derived: `quantity * unit_price`)

This structure supports common analytics:

- Revenue by product, category, customer, country, month
- Top customers / products by revenue

---

## 4. Config, Logging, and Run Modes

### 4.1 Configuration

Centralized in `src/config.py`:

- Paths:
  - `BASE_DIR`, `DATA_DIR`
  - `RAW_DATA_PATH`, `PROCESSED_DATA_PATH`
- Database:
  - Default: SQLite file `warehouse.db`
  - Overridable via `DB_URL` environment variable (loaded from `.env` if present)

This allows switching to Postgres/MySQL/etc. by changing configuration, not code.

### 4.2 Logging

Configured via `src/utils/logging_utils.py`:

- Uses Python `logging` with a custom `get_logger(name)` helper.
- Logs to:
  - Console (for local runs)
  - `logs/pipeline.log` (for history)
- Common format:
  - `timestamp | logger-name | level | message`

All core modules use this logger instead of `print()`.

### 4.3 Run Modes

- **Full run**:

  ```bash
  python -m src.main
