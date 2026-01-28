from pathlib import Path
import pandas as pd
import logging
from macvin.logging import setup_logging
from macvin.flows import get_paths, get_survey
import xarray as xr
import argparse

setup_logging(log_file="macvin.log")
logger = logging.getLogger(__name__)

strN = 25


def macvin_get_status(quick_run: bool = Falsecruise: str | None = None):

    logger.info("#### MACVIN STATUS FLOW ####")

    df = get_survey(cruise=cruise)

    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        bronze_dir = Path(row["RAW_files"])
        survey_status(silver_dir, bronze_dir, logger, cruise, quick_run)


def log_exists(logger, prefix, label, exists):
    log = logger.info if exists else logger.error
    log(f"{prefix} | {label:<18}: {exists}")


def get_time_bounds(nc_file, time_name="ping_time"):
    with xr.open_dataset(nc_file, decode_times=True, chunks={}) as ds:
        t = ds[time_name].values
    return t[0], t[-1]


def check_sv(preprocessed: Path, quick_run: bool = False):
    prefix = f"{str(preprocessed).split('/')[-5].ljust(strN)} | preprocessing         | Preprocessing used: {str(preprocessed).split('/')[-1].ljust(strN)}"
    sv_nc_files = sorted(list(preprocessed.glob("*.nc")))
    sv_nc = len(sv_nc_files)
    log_exists(logger, prefix, f"{sv_nc} nc files", sv_nc > 0)
    if sv_nc > 0 and not quick_run:
        check_monotonic(sv_nc_files, prefix)


def check_monotonic(sv_nc_files: list[Path], prefix: str):
    # Check the time vector
    bounds = []
    # Collect start and en times from netcdfs:
    for sv_file in sv_nc_files:
        t0, t1 = get_time_bounds(sv_file)
        bounds.append((sv_file, t0, t1))

    # check between files
    bad_pairs = []
    for (f_prev, _, t1_prev), (f_cur, t0_cur, _) in zip(bounds, bounds[1:]):
        if t0_cur <= t1_prev:
            bad_pairs.append((f_prev, f_cur, t1_prev, t0_cur))

    is_monotonic = len(bad_pairs) == 0

    if not is_monotonic:
        msg_lines = ["Non-monotonic time detected between files:"]
        for f1, f2, t_end, t_start in bad_pairs:
            msg_lines.append(
                f"File {f1.name} with t_end {t_end} ends later than start of {f2.name} with t_0 {t_start}"
            )
        msg = "\n".join(msg_lines)

        log_exists(logger, prefix, msg, is_monotonic)


def check_labels(target_classification: Path):
    # labels_nc
    labels_nc_files = sorted(list(target_classification.glob("*.nc")))
    labels_nc = len(labels_nc_files)
    prefix = f"{str(target_classification).split('/')[-6].ljust(strN)} | target_classification | Preprocessing used: korona_noisefiltering    "
    log_exists(logger, prefix, f"{labels_nc} nc files", labels_nc > 0)


def check_report(report: Path):
    # report
    luf = report / Path("ListUserFile26_.xml")

    # Zarr report
    zarr_report = report / Path("*_reports.zarr")
    _zreport = list(zarr_report.parent.glob(zarr_report.name))

    if _zreport:
        report_zarr = True
    else:
        report_zarr = False
    prefix = f"{str(report).split('/')[-7].ljust(strN)} | reportgenerator       | Preprocessing used: {str(report).split('/')[-3].ljust(strN)}"
    log_exists(logger, prefix, "Zarr store exist", report_zarr)
    log_exists(logger, prefix, "Luf file exist", luf.exists())


def check_idx(idxdata: Path):
    # labels_nc
    idxfiles = sorted(list(idxdata.glob("*.idx")))
    _str1 = "Directly from raw data".ljust(strN+20)
    _str2 = " idxfix".ljust(strN-2)
    prefix = f"{str(idxdata).split('/')[-5].ljust(strN)} |{_str2}| {_str1}"
    idx = len(idxfiles)
    log_exists(logger, prefix, f"{idx} idx files", idx > 0)


def check_raw(rawdata: Path):
    # labels_nc
    logger.info(rawdata)
    rawfiles = sorted(list(rawdata.glob("*.raw")))
    idxfiles = sorted(list(rawdata.glob("*.idx")))
    ek500files = sorted(list(rawdata.glob("*Data")))
    _str1 = "Raw data type".ljust(strN+20)
    _str2 = " NA".ljust(strN-2)
    
    prefix = f"{str(rawdata).split('/')[-4].ljust(strN)} |{_str2}| {_str1}"
    raw = len(rawfiles)
    idx = len(idxfiles)
    ek500 = len(ek500files)
    
    log_exists(logger, prefix, f"{raw} raw, {idx} idx, {ek500} ek500 ", raw+ek500+idx > 0)


def survey_status(silver_dir: Path, bronze_dir: Path, logger, cruise, quick_run):
    # Get the standard paths
    path_data = get_paths(silver_dir)

    # Check idx files
    check_raw(bronze_dir)

    # Check idx files
    check_idx(path_data['idxdata'])

    # Check sv_nc
    for _type in path_data['preprocessing'].keys():
        sv_dir = path_data['preprocessing'][_type]
        check_sv(sv_dir, quick_run)

    # Check atc
    check_labels(path_data['target_classification'])

    # Check reports
    for _type in path_data['reports'].keys():
        report = path_data['reports'][_type]
        check_report(report)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick-run", action="store_true")
    parser.add_argument("--cruise", type=str,
                        help="Cruise name to process, e.g. S1513S_PSCOTIA_MXHR6")
    args = parser.parse_args()
    macvin_get_status(quick_run=args.quick_run, cruise=args.cruise)
