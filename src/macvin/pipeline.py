import argparse
import logging
from pathlib import Path

from macvin.flows import (
    macvin_preprocessing_flow,
    macvin_reports_flow,
    macvin_lufreports_flow,
    macvin_idxprocessing_flow,
    macvin_convert_ek500_flow,
    macvin_atcprocessing_flow,
    atc2zarr_flow,
    preprocess2zarr_flow,
)
from macvin.analyzedata import macvin_consistency_flow
from macvin.logging import setup_logging

setup_logging(log_file="macvin.log")

logger = logging.getLogger(__name__)

SILVERDIR = Path("/data/s3/MACWIN-scratch/silver")

DEFAULT_CRUISE_HELP = "Cruise name to process, e.g. S1513S_PSCOTIA_MXHR6"


def run_flow(flow, *, cruise_required=False, extra_args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--silver-dir",
        type=Path,
        default=SILVERDIR,
        help=f"Path to silver directory (default: {SILVERDIR})",
    )
    parser.add_argument(
        "--cruise",
        type=str,
        required=cruise_required,
        help=DEFAULT_CRUISE_HELP,
    )

    if extra_args:
        extra_args(parser)

    args = parser.parse_args()
    kwargs = vars(args)

    flow(**kwargs)


def ek500conversion():
    run_flow(macvin_convert_ek500_flow)


def idxprocessing():
    run_flow(macvin_idxprocessing_flow)


def preprocessing():
    run_flow(macvin_preprocessing_flow, cruise_required=True)


def preprocess2zarr():
    run_flow(preprocess2zarr_flow)


def atcprocessing():
    run_flow(macvin_atcprocessing_flow)


def atc2zarr():
    run_flow(atc2zarr_flow)


def reports():
    run_flow(macvin_reports_flow)


def lufreports():
    run_flow(macvin_lufreports_flow)


def checkconsistency():
    run_flow(macvin_consistency_flow)
