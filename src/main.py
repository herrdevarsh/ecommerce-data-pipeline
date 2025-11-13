import argparse
from src.orchestration.pipeline import run_pipeline
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="E-commerce data pipeline"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run ingestion + transform + quality checks, but skip database load.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger.info("Starting pipeline (dry_run=%s)", args.dry_run)
    run_pipeline(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
