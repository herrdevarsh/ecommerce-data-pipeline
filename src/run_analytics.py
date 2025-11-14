import argparse

from src.analytics.reports import generate_all_reports
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate analytics reports from the data warehouse"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of top products to include in the top products report (default: 10)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger.info("Running analytics reports (limit=%d)", args.limit)
    generate_all_reports(limit=args.limit)
    logger.info("Analytics reports completed.")


if __name__ == "__main__":
    main()
