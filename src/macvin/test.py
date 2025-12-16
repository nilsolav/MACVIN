from macvin.flows import macvin_test_flow
import argparse


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    print(f"dry_run = {args.dry_run}")
    
    macvin_test_flow(dry_run=args.dry_run)
