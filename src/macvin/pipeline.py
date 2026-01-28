import logging
from macvin.flows import (
    macvin_preprocessing_flow,
    macvin_reports_flow,
    macvin_lufreports_flow,
    macvin_idxprocessing_flow)
import argparse
from macvin.logging import setup_logging

setup_logging(log_file="macvin.log")

logger = logging.getLogger(__name__)


def idxprocessing():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cruise", type=str,
                        help="Cruise name to process, e.g. S1513S_PSCOTIA_MXHR6")
    args = parser.parse_args()
    macvin_idxprocessing_flow(dry_run=args.dry_run, cruise=args.cruise)


def preprocessing():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cruise", type=str,
                        help="Cruise name to process, e.g. S1513S_PSCOTIA_MXHR6")
    args = parser.parse_args()
    macvin_preprocessing_flow(dry_run=args.dry_run, cruise=args.cruise)


def reports():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cruise", type=str,
                        help="Cruise name to process, e.g. S1513S_PSCOTIA_MXHR6")
    args = parser.parse_args()
    macvin_reports_flow(dry_run=args.dry_run, cruise=args.cruise)


def lufreports():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--cruise", type=str,
                        help="Cruise name to process, e.g. S1513S_PSCOTIA_MXHR6")
    args = parser.parse_args()
    macvin_lufreports_flow(dry_run=args.dry_run, cruise=args.cruise)
