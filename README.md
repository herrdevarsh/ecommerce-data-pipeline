# E-commerce Data Pipeline 🧱

End-to-end **batch data pipeline** project built to showcase practical data engineering skills:

- Ingests raw e-commerce orders from CSV
- Cleans, transforms and validates data with pandas
- Loads a **star schema** (fact + dimensions) into SQLite (easily swappable to Postgres)
- Runs analytics queries and exports **reports** for business users
- Includes **data quality checks**, **logging**, **CLI with dry-run**, **tests**, and **CI with GitHub Actions**

> Built as a portfolio project for data engineer roles – designed to be small but realistic and easy to explain in interviews.

---

## 1. Project Overview

**Use case**

Simulate a real-world e-commerce analytics setup:

- Raw orders land in a CSV file
- We need clean, validated data in a warehouse
- Analysts want:
  - Top products by revenue
  - Revenue by month
  - Revenue by country / customer

**What this repo demonstrates**

- End-to-end pipeline: **ingest → transform → validate → load → report**
- Proper **project structure** (`src/`, `tests/`, `sql/`, `docs/`)
- **Dimensional modeling**: fact table + dimension tables
- **Data quality layer** that can fail the pipeline on bad data
- **Observability**:
  - structured logging to file + console
  - JSON run summary
- **Analytics layer** on top of the warehouse
- **Automated tests** and **GitHub Actions CI**

---

## 2. Tech Stack

- **Language**: Python 3.x  
- **Data processing**: pandas
- **Storage / warehouse**: SQLite (via SQLAlchemy, easily switchable to other DBs)
- **Orchestration**: simple Python CLI (could be wrapped in Airflow/Prefect later)
- **Config**: `python-dotenv` + centralized `config.py`
- **Testing**: pytest
- **CI**: GitHub Actions (`.github/workflows/ci.yml`)

---

## 3. Data & Model

### 3.1 Raw data

File: `data/raw/orders_raw.csv`

Columns:

- `order_id`
- `customer_id`, `customer_name`, `country`
- `product_id`, `product_name`, `category`
- `order_date`
- `quantity`
- `unit_price`

Current sample dataset (for demo):

- ~40+ orders
- Multiple customers across several countries (Germany, France, Italy, Spain, …)
- Multiple products & categories (Electronics, Accessories, Stationery, Home)
- Date range across multiple months in 2024

### 3.2 Warehouse schema (star schema)

Data is loaded into a simple star schema inside `warehouse.db`:

**Dimension: `dim_customers`**

- `customer_id` (PK)
- `customer_name`
- `country`

**Dimension: `dim_products`**

- `product_id` (PK)
- `product_name`
- `category`

**Fact: `fact_orders`**

- `order_id` (PK)
- `customer_id` (FK → `dim_customers.customer_id`)
- `product_id` (FK → `dim_products.product_id`)
- `order_date`
- `quantity`
- `unit_price`
- `total_amount` (`quantity * unit_price`)

This model supports common analytics like:

- Revenue by product / category
- Revenue by month
- Revenue by country / customer

---

## 4. Project Structure

```bash
ecommerce-data-pipeline/
├─ data/
│  ├─ raw/              # Input CSV (orders_raw.csv)
│  └─ processed/        # Cleaned CSV (orders_clean.csv)
├─ docs/
│  └─ design.md         # Detailed design document
├─ logs/
│  ├─ pipeline.log      # Structured logs
│  └─ run_summary.json  # JSON summary of last run
├─ reports/
│  ├─ top_products_by_revenue.csv
│  ├─ revenue_by_month.csv
│  └─ revenue_by_country.csv
├─ sql/
│  └─ analytics_queries.sql   # Reference SQL for analytics
├─ src/
│  ├─ analytics/
│  │  └─ reports.py           # Reporting/analytics functions
│  ├─ ingestion/
│  │  └─ ingest_orders.py     # Read raw CSV
│  ├─ transformations/
│  │  ├─ transform_orders.py  # Clean + feature engineering
│  │  └─ data_quality.py      # Data validation rules
│  ├─ warehouse/
│  │  ├─ db.py                # SQLAlchemy engine
│  │  └─ load_to_db.py        # Create schema + load tables
│  ├─ orchestration/
│  │  └─ pipeline.py          # Orchestrates pipeline steps
│  ├─ utils/
│  │  └─ logging_utils.py     # Console + file logger
│  ├─ config.py               # Paths, DB_URL, dirs
│  ├─ main.py                 # CLI entrypoint for pipeline
│  └─ run_analytics.py        # CLI entrypoint for reports
├─ tests/
│  ├─ test_transformations.py
│  ├─ test_data_quality.py
│  └─ test_analytics.py       # Analytics queries on in-memory DB
├─ .github/workflows/ci.yml   # GitHub Actions: run tests
├─ .gitignore
├─ requirements.txt
└─ README.md
