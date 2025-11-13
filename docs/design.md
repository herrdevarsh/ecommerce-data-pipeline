# Design Notes

## Goals

- Simple end-to-end batch pipeline for e-commerce orders.
- Demonstrate ingestion, transformation, loading, and SQL analytics.
- Keep dependencies light and easy to run locally.

## Choices

- **SQLite** instead of Postgres: zero setup, easy to run anywhere.
- **Single fact table** instead of full star schema to keep the project small and focused.
- **pandas** for transformations: standard tool in data engineering and data science.

## Future Improvements

- Add dimension tables (customers, products).
- Use Airflow or Prefect for orchestration.
- Store data in cloud storage (S3, GCS).
- Use Spark for large datasets.
