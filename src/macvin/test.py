from macvin.flows import macvin_test_flow
import argparse
from macvin.logging import setup_logging
import logging

setup_logging(log_file="macvin.log")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    print(f"dry_run = {args.dry_run}")

    macvin_test_flow(dry_run=args.dry_run)
