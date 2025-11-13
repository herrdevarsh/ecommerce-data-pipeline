from pathlib import Path

# BASE_DIR = project root (ecommerce-data-pipeline)
BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "orders_raw.csv"
PROCESSED_DATA_PATH = DATA_DIR / "processed" / "orders_clean.csv"

DB_PATH = BASE_DIR / "warehouse.db"
DB_URL = f"sqlite:///{DB_PATH}"
