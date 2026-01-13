import logging
from macvin.flows import macvin_full_flow, macvin_reports_flow


def main():
    macvin_full_flow()


def reports():
    macvin_reports_flow()
