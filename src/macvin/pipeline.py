import logging
from macvin.flows import macvin_full_flow, macvin_reports_flow
import argparse


def main():
    macvin_full_flow()


def reports():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    macvin_reports_flow(dry_run=args.dry_run)
