import logging
from macvin.flows import macvin_full_flow, macvin_reports_flow
import argparse
from macvin.logging import setup_logging

setup_logging(log_file="macvin.log")
logger = logging.getLogger(__name__)


def main():
    macvin_full_flow()


def reports():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    macvin_reports_flow(dry_run=args.dry_run)
