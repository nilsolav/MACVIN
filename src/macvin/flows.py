from pathlib import Path
from macvin.tasks import (
    korona_noisefiltering,
    korona_preprocessing,
    mackerel_korneliussen2016,
    reportgeneration_zarr,
    korona_fixidx,
    run_zarr2lufxml,
)
import pandas as pd
import logging
import platform
import os
import subprocess
import tempfile
from fnmatch import fnmatch

logger = logging.getLogger(__name__)

# ------------------
# Helper functions
# ------------------


def get_paths(silver_dir: Path) -> dict:
    dat = {}
    dat["idxdata"] = silver_dir / Path("EK_RAWDATA", "korona_fixidx")

    dat["preprocessing"] = {
        "noisefiltering": silver_dir / Path("PREPROCESSING", "korona_noisefiltering"),
        "preprocessing": silver_dir / Path("PREPROCESSING", "korona_preprocessing"),
    }

    dat["target_classification"] = silver_dir / Path(
        "TARGET_CLASSIFICATION",
        "korona_noisefiltering",
        "mackerel_korneliussen2016",
    )

    dat["quality_control"] = silver_dir / Path(
        "QUALITY_CONTROL", "korona_datacompression"
    )
    dat["bottom_detection"] = silver_dir
    dat["reports"] = {
        "noisefiltering": silver_dir
        / Path(
            "REPORTS",
            "korona_noisefiltering",
            "mackerel_korneliussen2016",
            "reportgeneration-zarr",
        ),
        "preprocessing": silver_dir
        / Path(
            "REPORTS",
            "korona_preprocessing",
            "mackerel_korneliussen2016",
            "reportgeneration-zarr",
        ),
    }
    return dat


def luf_parameters():
    par = {
        "cruise": "NA",
        "version_number": "NA",
        "commit_sha": "NA",
        "PingAxisInterval": 0.1,
        "LogValidity": "V",
        "Type": "C",
        "Unit": "m2nmi-2",
        "ChannelDepthInterval": 10,
        "SvThreshold": -82.0,
        "frequency": 200000,
        "IntegrationType": "threshold",  # Could also be 'proportion'
        "threshold": 0.75,
        "seabed_pad": 10,
        "Instrument": "ID_F38",
        "Calibration": "C0",
        "DataAcquisition": "DA0",
        "DataProcessing": "DP38",
        "AC_PingAxisIntervalType": "AC_PingAxisIntervalType_distance",
        "AC_PingAxisIntervalUnit": "AC_PingAxisIntervalUnit_nmi",
        "AC_PingAxisIntervalOrigin": "AC_PingAxisIntervalOrigin_start",
        "TransducerOrientation": "downwards-looking",
        "category": ['1000004', '1000005'],
        "remove_nan_in_position": False,
        "Code": "NA",
        "LocalID": "NA",
    }

    return par


def get_survey(cruise: str | None = None) -> tuple[pd.DataFrame, set]:
    """
    Load the cruises table and optionally filter by cruise name.

    Args:
        cruise: Exact cruise ID to filter on, e.g. "S1513S_PSCOTIA_MXHR6".
                If None, returns the full dataframe.

    Raises:
        ValueError: If a cruise string is given that does not appear in the CSV.

    Returns:
        A pandas DataFrame containing matching rows.
    """
    df = pd.read_csv("cruises.csv")
    exclude_files = set(pd.read_csv("excludefiles.csv")['excluded_files'])
    
    if cruise is not None:
        # Check existence first
        if cruise not in df["cruise"].values:
            raise ValueError(f"cruise '{cruise}' not found in cruises.csv")

        # Then filter
        df = df[df["cruise"] == cruise]

    return df, exclude_files


def _make_filtered_source_dir(
    source_dir: Path,
    exclude_names: set[str],
    pattern: str = "*",
    link_mode: str = "symlink",  # "symlink" or "hardlink" or "copy"
) -> Path:
    """
    Create a temporary directory containing only links (or copies) to allowed files.
    Returns the temp dir Path. Caller must keep the TemporaryDirectory alive.
    """
    
    files = list(source_dir.glob("*"))
    remove = [
        f for f in files
        if any(fnmatch(f.name, pat) for pat in exclude_names)
    ]
    allowed = [f for f in files if f not in remove]
    logger.info(f"Total number of files: {len(files)}, and allowed numbers of files {len(allowed)}")
    
    if not allowed:
        raise RuntimeError(f"No files matched {pattern} in {source_dir} after exclusions")

    tmpdir_obj = tempfile.TemporaryDirectory(prefix="ek500_filtered_")
    tmpdir = Path(tmpdir_obj.name)

    # attach object so it doesn't get GC'd while tmpdir is in use
    #tmpdir._tmpdir_obj = tmpdir_obj  # type: ignore[attr-defined]

    for src in allowed:
        dst = tmpdir / src.name
        if link_mode == "symlink":
            dst.symlink_to(src)
        elif link_mode == "hardlink":
            # requires same filesystem; fails across mounts
            os.link(src, dst)
        elif link_mode == "copy":
            dst.write_bytes(src.read_bytes())
        else:
            raise ValueError(f"Unknown link_mode: {link_mode}")

    return tmpdir, tmpdir_obj


# ------------------
# Main flow functions
# ------------------

def macvin_convert_ek500_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN EK500 FLOW ####")

    df, exclude_files = get_survey(cruise=cruise)
    for idx, row in df.iterrows():
        cruise = row["cruise"]
        if "BEI" in row["Original_RAW_files"]:

            try:
                logger.info(f"{cruise}: Converting ek 500 data")
                logger.debug(f"idx tools from {row['Original_RAW_files']} to {row['RAW_files']}")

                batch = Path(os.getenv("LSSS")) / "korona/KoronaCli.sh"

                source_dir = Path(row["Original_RAW_files"])
                dest_dir = Path(row["RAW_files"])

                # Build a curated source dir containing only allowed *.ek5
                filtered_source, tmpobj = _make_filtered_source_dir(
                    source_dir=source_dir,
                    exclude_names=exclude_files,
                    pattern="*",
                    link_mode="symlink",
                )

                cmd = [
                    str(batch),
                    "batch",
                    "--max-parallel", "5",
                    "--destination", str(dest_dir),
                    "--source", str(filtered_source),
                ]

                logger.info("Running: %s", " ".join(cmd))

                if not dry_run:
                    subprocess.run(cmd, check=True)

                # Important: keep filtered_source alive until run completes.
                # The TemporaryDirectory object is attached to filtered_source.
                # When filtered_source goes out of scope, it will clean up automatically.

            except Exception:
                logger.exception("EK500 conversion failed")

            finally:
                tmpobj.cleanup()

        else:
            logger.info(f"{cruise} does not contatin EK 500 data")


def macvin_idxprocessing_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN IDXFIX FLOW ####")

    basedir = Path("/data/s3/MACWIN-scratch")
    df, exclude_files = get_survey(cruise=cruise)
    
    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        path_data = get_paths(silver_dir)

        try:
            logger.info("# 0. idx tools")
            logger.debug("Removing old idx files.")
            idx_dir = Path(path_data["idxdata"])
            for f in idx_dir.glob("*.idx"):
                f.unlink()

            logger.info(f"idx tools from {row['RAW_files']} to {path_data['idxdata']}")
            korona_fixidx(
                idx=row["RAW_files"],
                preprocessing=path_data[
                    "idxdata"
                ],  # Generate the updated idx files into idxdata
                dry_run=dry_run,
            )

        except Exception:
            # Full traceback goes into logs
            logger.exception("Fix idx failed for this case — continuing with next case")


def macvin_lufreports_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN LUF REPORTS FLOW ####")

    df, exclude_files = get_survey(cruise=cruise)
    
    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        path_data = get_paths(silver_dir)

        for _type in path_data["reports"].keys():
            zreport = path_data["reports"][_type] / "*_reports.zarr"
            _zreport = list(zreport.parent.glob(zreport.name))
            par = luf_parameters()
            par["Code"] = cruise
            if _zreport:
                try:
                    logger.info(
                        f"{cruise} Run the luf export for {str(path_data['reports'][_type]).split('/')[-3]}"
                    )
                    luf_report = _zreport[0].parent / "ListUserFile26_.xml"
                    logger.debug(f"luf report file: {luf_report}")
                    run_zarr2lufxml(
                        zarr_report=_zreport[0],
                        luf_report=luf_report,
                        par=par,
                        dry_run=dry_run,
                    )
                except Exception as e:
                    logger.error(
                        "f{cruise} Preprocessing pipeline failed for {str(reports[_type]).split('/')[-3]}"
                    )
                    logger.error(e)
            else:
                logger.error(
                    f"{cruise} Zarr report does not exist for {str(path_data['reports'][_type]).split('/')[-3]}"
                )


def macvin_reports_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN REPORTS FLOW ####")

    df, exclude_files = get_survey(cruise=cruise)
    
    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        path_data = get_paths(silver_dir)
        logger.info(cruise)

        for _type in path_data["reports"].keys():
            # Check if report exists:
            zreport = path_data["reports"][_type] / "*_reports.zarr"
            _zreport = list(zreport.parent.glob(zreport.name))

            if not _zreport:
                try:
                    logger.info(
                        f"Report does not exist : {str(path_data['reports'][_type]).split('/')[-3]}"
                    )
                    reportgeneration_zarr(
                        preprocessing=path_data["preprocessing"][_type],
                        target_classification=path_data["target_classification"],
                        bottom_detection=path_data["bottom_detection"],
                        cruise=cruise,
                        reports=path_data["reports"][_type],
                        dry_run=dry_run,
                    )
                except Exception:
                    logger.info(
                        "Preprocessing pipeline failed for this case — continuing with next case"
                    )
            else:
                logger.info(
                    f"Report exists         : {str(path_data['reports'][_type]).split('/')[-3]}"
                )


def macvin_preprocessing_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN PREPROCESSING FLOW ####")

    df, exclude_files = get_survey(cruise=cruise)

    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        rerun = row["status"] not in ("OK", "FAIL")
        bronze_dir = Path(row["RAW_files"])
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")

        logger.info(cruise)
        if rerun:
            logger.info(f"Bronze dir: {bronze_dir}")
            logger.info(f"Bronze dir is available : {bronze_dir.exists()}")
            logger.info(f"Silver dir: {silver_dir}")
            preprocessing_flow(
                cruise=str(cruise),
                bronze_dir=bronze_dir,
                silver_dir=silver_dir,
                dry_run=dry_run,
            )
        else:
            logger.info(
                f"Cruise is already processed. Remove {row['status']} from cruises.csv to rerun processing."
            )


def macvin_atcprocessing_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN ATCPROCESSING FLOW ####")

    df, exclude_files = get_survey(cruise=cruise)

    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        rerun = row["status"] not in ("OK", "FAIL")
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")

        logger.info(cruise)
        if rerun:
            logger.info(f"Silver dir: {silver_dir}")
            atcprocessing_flow(
                cruise=str(cruise),
                silver_dir=silver_dir,
                dry_run=dry_run,
            )
        else:
            logger.info(
                f"Cruise is already processed. Remove {row['status']} from cruises.csv to rerun processing."
            )


def macvin_test_flow(dry_run: bool = True):
    logger.info("#### MACVIN TEST PROCESSING FLOW ####")
    if platform.node() == "HI-14667":
        basedir = Path("/crimac-scratch")
    else:
        basedir = Path("/data", "crimac-scratch")
    logger.info(basedir)
    cruise = Path("S2005114_PGOSARS_4174")
    bronze_dir = (
        basedir
        / Path("test_data_azure")
        / cruise
        / Path("ACOUSTIC", "EK")
        / Path("EK_RAWDATA")
        / Path("rawdata")
    )
    logger.info(f"Bronze dir : {bronze_dir}")
    silver_dir = (
        basedir / Path("test_data_azure_silver") / cruise / Path("ACOUSTIC", "EK")
    )
    logger.info(f"Silver dir : {silver_dir}")
    
    preprocessing_flow(
        cruise=str(cruise),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        dry_run=dry_run,
    )

    atcprocessing_flow(
        cruise=str(cruise),
        silver_dir=silver_dir,
        dry_run=dry_run,
    )


# ------------------
# Flows per survey
# ------------------


def preprocessing_flow(
    cruise: str,
    bronze_dir: Path,
    silver_dir: Path,
    dry_run: bool = False,
):
    logger.info(f"#### {cruise} ####")
    rawdata = bronze_dir
    path_data = get_paths(silver_dir)

    try:
        logger.info("# 0. idx fix")

        korona_fixidx(
            idx=rawdata,
            preprocessing=path_data[
                "idxdata"
            ],  # Generate the updated idx files into idxdata
                dry_run=dry_run,
        )

    except Exception:
        # Full traceback goes into logs
        logger.exception(
            "idx fix pipeline failed for this case — continuing with next case"
        )

    try:
        logger.info("# 1a. Noise filtering")
        korona_noisefiltering(
            idxdata=path_data["idxdata"],
            rawdata=rawdata,
            preprocessing=path_data["preprocessing"]["noisefiltering"],
            dry_run=dry_run,
        )
    except Exception:
        # Full traceback goes into logs
        logger.exception(
            "Preprocessing pipeline failed for this case — continuing with next case"
        )

    try:
        logger.info("# 1c. Preprocesing")
        korona_preprocessing(
            idxdata=path_data["idxdata"],
            rawdata=rawdata,
            preprocessing=path_data["preprocessing"]["preprocessing"],
            dry_run=dry_run,
        )
    except Exception:
        # Full traceback goes into Prefect logs
        logger.exception(
            "Preprocessing pipeline failed for this case — continuing with next case"
        )


def atcprocessing_flow(
    cruise: str,
    silver_dir: Path,
    dry_run: bool = False,
):

    logger.info(f"#### {cruise} ####")
    path_data = get_paths(silver_dir)

    try:
        logger.info("# 2. Target classification")
        mackerel_korneliussen2016(
            preprocessing=path_data["preprocessing"]["preprocessing"],
            target_classification=path_data["target_classification"],
            dry_run=dry_run,
        )

    except Exception:
        # Full traceback goes into Prefect logs
        logger.exception(
            "ATC processing pipeline failed for this case — continuing with next case"
        )

