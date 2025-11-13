from sqlalchemy import create_engine
from src.config import DB_URL

def get_engine():
    """
    Return a SQLAlchemy engine for the SQLite database.
    """
    return create_engine(DB_URL, echo=False, future=True)
