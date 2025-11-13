import logging
from src.config import LOGS_DIR

def get_logger(name: str) -> logging.Logger:
    """
    Return a logger that logs to both console and a log file.
    """
    logger = logging.getLogger(name)

    # Avoid adding multiple handlers if get_logger is called multiple times
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # Ensure logs directory exists
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / "pipeline.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Don't propagate to root logger
    logger.propagate = False

    return logger
