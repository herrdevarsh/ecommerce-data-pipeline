## Features

- **End-to-end batch pipeline**: ingest → transform → validate → load.
- **Star schema**: `fact_orders` + `dim_customers` + `dim_products`.
- **Configurable database**: defaults to local SQLite (`warehouse.db`) but can be switched to any SQL database by setting `DB_URL` in a `.env` file.
- **Structured logging**: all steps log to both console and `logs/pipeline.log`.
- **Data quality checks**:
  - No null or duplicate `order_id`
  - No non-positive quantities or unit prices
- **Tests**: pytest tests for transformation logic and data quality validation.

