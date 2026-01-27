from pathlib import Path
import pandas as pd
import logging
from macvin.logging import setup_logging
from macvin.flows import get_paths
import xarray as xr

setup_logging(log_file="macvin.log")
logger = logging.getLogger(__name__)

strN = 25


def macvin_get_status():
    logger.info("#### MACVIN STATUS FLOW ####")

    df = pd.read_csv("cruises.csv")

    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        survey_status(silver_dir, logger, cruise)


def log_exists(logger, prefix, label, exists):
    log = logger.info if exists else logger.error
    log(f"{prefix} | {label:<18}: {exists}")


def get_time_bounds(nc_file, time_name="ping_time"):
    with xr.open_dataset(nc_file, decode_times=True, chunks={}) as ds:
        t = ds[time_name].values
    return t[0], t[-1]


def check_sv(preprocessed: Path):
    prefix = f"{str(preprocessed).split('/')[-5].ljust(strN)} | preprocessing         | Preprocessing used: {str(preprocessed).split('/')[-1].ljust(strN)}"
    sv_nc_files = sorted(list(preprocessed.glob("*.nc")))
    sv_nc = len(sv_nc_files)
    log_exists(logger, prefix, f"{sv_nc} nc files", sv_nc > 0)
    if sv_nc > 0:
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
    prefix = f"{str(idxdata).split('/')[-7].ljust(strN)} | idxfix                | Directly from raw data "
    idx = len(idxfiles)
    log_exists(logger, prefix, f"{idx} idx files", idx > 0)


def survey_status(silver_dir: Path, logger, cruise):
    # Get the standard paths
    (
        idxdata,
        preprocessing,
        target_classification,
        quality_control,
        bottom_detection,
        reports,
    ) = get_paths(silver_dir)

    # Check idx files
    check_idx(idxdata)

    # Check sv_nc
    for _type in preprocessing.keys():
        sv_dir = preprocessing[_type]
        check_sv(sv_dir)

    # Check atc
    check_labels(target_classification)

    # Check reports
    for _type in reports.keys():
        report = reports[_type]
        check_report(report)


def main():
    macvin_get_status()
