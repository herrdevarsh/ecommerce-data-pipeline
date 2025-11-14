from pathlib import Path
import os
from dotenv import load_dotenv

# Load variables from .env file if present
load_dotenv()

# Project root directory (ecommerce-data-pipeline)
BASE_DIR = Path(__file__).resolve().parents[1]

# Data paths
DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "orders_raw.csv"
PROCESSED_DATA_PATH = DATA_DIR / "processed" / "orders_clean.csv"

# Database configuration
DB_PATH = BASE_DIR / "warehouse.db"
# You can override this with an environment variable DB_URL
DB_URL = os.getenv("DB_URL", f"sqlite:///{DB_PATH}")

# Logs directory
LOGS_DIR = BASE_DIR / "logs"

# Reports directory
REPORTS_DIR = BASE_DIR / "reports"
