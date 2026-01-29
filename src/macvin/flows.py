from pathlib import Path
from macvin.tasks import (
    korona_noisefiltering,
    korona_preprocessing,
    korona_datacompression,
    mackerel_korneliussen2016,
    reportgeneration_zarr,
    korona_fixidx,
    run_zarr2lufxml,
)
import pandas as pd
import logging
import platform

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
        "datacompression": silver_dir / Path("PREPROCESSING", "korona_datacompression"),
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
        "datacompression": silver_dir
        / Path(
            "REPORTS",
            "korona_datacompression",
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


def get_survey(cruise: str | None = None) -> pd.DataFrame:
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

    if cruise is not None:
        # Check existence first
        if cruise not in df["cruise"].values:
            raise ValueError(f"cruise '{cruise}' not found in cruises.csv")

        # Then filter
        df = df[df["cruise"] == cruise]

    return df


# ------------------
# Main flow functions
# ------------------


def macvin_idxprocessing_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN IDXFIX FLOW ####")

    basedir = Path("/data/s3/MACWIN-scratch")
    df = get_survey(cruise=cruise)

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        path_data = get_paths(silver_dir)

        try:
            logger.info("# 0. idx tools")
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
    logger.info("#### MACVIN REPORTS FLOW ####")

    df = get_survey(cruise=cruise)

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

    df = get_survey(cruise=cruise)

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
                    # Full traceback goes into Prefect logs
                    logger.info(
                        "Preprocessing pipeline failed for this case — continuing with next case"
                    )
            else:
                logger.info(
                    f"Report exists         : {str(path_data['reports'][_type]).split('/')[-3]}"
                )


def macvin_preprocessing_flow(dry_run: bool = False, cruise: str | None = None):
    logger.info("#### MACVIN FULL FLOW ####")

    df = get_survey(cruise=cruise)

    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        rerun = row["status"] != "OK"
        bronze_dir = Path(row["RAW_files"])
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")

        logger.info(cruise)
        if rerun:
            logger.info(f"Bronze dir: {bronze_dir}")
            logger.info(f"Bronze dir is available : {bronze_dir.exists()}")
            logger.info(f"Silver dir: {silver_dir}")
            survey_flow(
                cruise=str(cruise),
                bronze_dir=bronze_dir,
                silver_dir=silver_dir,
                dry_run=dry_run,
            )
        else:
            logger.info(
                "Cruise is already processed. Remove 'OK' from cruises.csv to rerun processing."
            )


def macvin_test_flow(dry_run: bool = True):
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
    survey_flow(
        cruise=str(cruise),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        dry_run=dry_run,
    )


# ------------------
# Flows per survey
# ------------------


def survey_flow(
    cruise: str,
    bronze_dir: Path,
    silver_dir: Path,
    dry_run: bool = False,
):
    logger.info(f"#### {cruise} ####")
    rawdata = bronze_dir
    path_data = get_paths(silver_dir)

    futures = []
    notfailed = True

    if notfailed:
        try:
            logger.info("# 1a. Noise filtering")
            futures.append(
                korona_noisefiltering(
                    idxdata=path_data["idxdata"],
                    rawdata=rawdata,
                    preprocessing=path_data["preprocessing"]["noisefiltering"],
                    dry_run=dry_run,
                )
            )
        except Exception:
            # Full traceback goes into logs
            notfailed = False
            logger.exception(
                "Preprocessing pipeline failed for this case — continuing with next case"
            )

    if notfailed:
        try:
            logger.info("# 1b. Data compression (use a sub flow since it is 2 steps")
            futures.append(
                datacompression_flow(
                    idxdata=path_data["idxdata"],
                    rawdata=rawdata,
                    preprocessing=path_data["preprocessing"]["datacompression"],
                    quality_control=path_data["quality_control"],
                    dry_run=dry_run,
                )
            )
        except Exception:
            # Full traceback goes into Prefect logs
            notfailed = False
            logger.exception(
                "Preprocessing pipeline failed for this case — continuing with next case"
            )
    if notfailed:
        try:
            logger.info("# 1c. Preprocesing")
            futures.append(
                korona_preprocessing(
                    idxdata=path_data["idxdata"],
                    rawdata=rawdata,
                    preprocessing=path_data["preprocessing"]["preprocessing"],
                    dry_run=dry_run,
                )
            )
        except Exception:
            # Full traceback goes into Prefect logs
            notfailed = False
            logger.exception(
                "Preprocessing pipeline failed for this case — continuing with next case"
            )
    if notfailed:
        try:
            logger.info("# 2. Target classification")
            mackerel_korneliussen2016(
                idxdata=path_data["idxdata"],
                rawdata=rawdata,
                target_classification=path_data["target_classification"],
                dry_run=dry_run,
            )

        except Exception:
            # Full traceback goes into Prefect logs
            notfailed = False
            logger.exception(
                "Preprocessing pipeline failed for this case — continuing with next case"
            )


def datacompression_flow(
    idxdata: Path,
    rawdata: Path,
    preprocessing: Path,
    quality_control: Path,
    dry_run: bool = False,
):
    """Two steps for the data compression"""
    logger.info("# 2a. Data compression: raw -> raw")
    korona_datacompression(
        idxdata=idxdata,
        rawdata=rawdata,
        preprocessing=preprocessing / Path("tmp"),
        quality_control=quality_control,
        dry_run=dry_run,
    )

    logger.info("# 2b. Data preprocessing: raw -> nc")
    korona_preprocessing(
        idxdata=preprocessing / Path("tmp"),
        rawdata=preprocessing / Path("tmp"),
        preprocessing=preprocessing,
        dry_run=dry_run,
    )
