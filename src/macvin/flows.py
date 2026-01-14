from pathlib import Path
from macvin.tasks import (
    korona_noisefiltering,
    korona_preprocessing,
    korona_datacompression,
    mackerel_korneliussen2016,
    reportgeneration_zarr,
)
import pandas as pd
import logging


logger = logging.getLogger(__name__)


def get_paths(silver_dir):
    preprocessing = {
        "noisefiltering": silver_dir / Path("PREPROCESSING", "korona_noisefiltering"),
        "preprocessing": silver_dir / Path("PREPROCESSING", "korona_preprocessing"),
        "datacompression": silver_dir / Path("PREPROCESSING", "korona_datacompression"),
    }

    target_classification = silver_dir / Path(
        "TARGET_CLASSIFICATION",
        "korona_noisefiltering",
        "mackerel_korneliussen2016",
    )

    quality_control = silver_dir / Path("QUALITY_CONTROL", "korona_datacompression")
    bottom_detection = silver_dir
    reports = {
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
    return (
        preprocessing,
        target_classification,
        quality_control,
        bottom_detection,
        reports,
    )


def macvin_reports_flow(dry_run: bool = False):
    logger.info("#### MACVIN REPORTS FLOW ####")

    df = pd.read_csv("cruises.csv")[:]

    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        (
            preprocessing,
            target_classification,
            quality_control,
            bottom_detection,
            reports,
        ) = get_paths(silver_dir)
        logger.info(cruise)

        for _type in reports.keys():
            # Check if report exists:
            zreport = reports[_type] / "*_reports.zarr"
            _zreport = list(zreport.parent.glob(zreport.name))

            if not _zreport:
                try:
                    logger.info(
                        f"Report does not exist : {str(reports[_type]).split('/')[-3]}"
                    )
                    reportgeneration_zarr(
                        preprocessing=preprocessing[_type],
                        target_classification=target_classification,
                        bottom_detection=bottom_detection,
                        cruise=cruise,
                        reports=reports[_type],
                        dry_run=dry_run,
                    )
                except Exception:
                    # Full traceback goes into Prefect logs
                    logger.info(
                        "Preprocessing pipeline failed for this case — continuing with next case"
                    )
            else:
                logger.info(
                    f"Report exists         : {str(reports[_type]).split('/')[-3]}"
                )


def macvin_full_flow(dry_run: bool = False):
    logger.info("#### MACVIN FULL FLOW ####")

    df = pd.read_csv("cruises.csv")[5:]

    basedir = Path("/data/s3/MACWIN-scratch")

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        bronze_dir = Path(row["RAW_files"])
        silver_dir = basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")

        logger.info(cruise)
        logger.info(f"Bronze dir: {bronze_dir}")
        logger.info(f"Bronze dir is available : {bronze_dir.exists()}")
        logger.info(f"Silver dir: {silver_dir}")
        survey_flow(
            cruise=str(cruise),
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            dry_run=dry_run,
        )


def macvin_test_flow(dry_run: bool = True):
    basedir = Path("/data/s3/MACWIN-scratch/")
    cruise = Path("S2005114_PGOSARS_4174")
    bronze_dir = (
        basedir
        / Path("test_data_azure")
        / cruise
        / Path("ACOUSTIC", "EK")
        / Path("EK_RAWDATA")
    )
    silver_dir = (
        basedir / Path("test_data_azure_silver") / cruise / Path("ACOUSTIC", "EK")
    )

    survey_flow(
        cruise=str(cruise),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        dry_run=dry_run,
    )


def survey_flow(
    cruise: str,
    bronze_dir: Path,
    silver_dir: Path,
    dry_run: bool = False,
):
    logger.info(f"#### {cruise} ####")
    rawdata = bronze_dir

    preprocessing, target_classification, quality_control, bottom_detection, reports = (
        get_paths(silver_dir)
    )

    futures = []
    try:
        logger.info("# 1a. Noise filtering")
        futures.append(
            korona_noisefiltering(
                rawdata=rawdata,
                preprocessing=preprocessing["noisefiltering"],
                dry_run=dry_run,
            )
        )
    except Exception:
        # Full traceback goes into Prefect logs
        logger.exception(
            "Preprocessing pipeline failed for this case — continuing with next case"
        )

    try:
        logger.info("# 1b. Data compression (use a sub flow since it is 2 steps")
        futures.append(
            datacompression_flow(
                rawdata=rawdata,
                preprocessing=preprocessing["datacompression"],
                quality_control=quality_control,
                dry_run=dry_run,
            )
        )
    except Exception:
        # Full traceback goes into Prefect logs
        logger.exception(
            "Preprocessing pipeline failed for this case — continuing with next case"
        )

    try:
        logger.info("# 1c. Preprocesing")
        futures.append(
            korona_preprocessing(
                rawdata=rawdata,
                preprocessing=preprocessing["preprocessing"],
                dry_run=dry_run,
            )
        )
    except Exception:
        # Full traceback goes into Prefect logs
        logger.exception(
            "Preprocessing pipeline failed for this case — continuing with next case"
        )

    try:
        logger.info("# 2. Target classification")
        futures.append(
            mackerel_korneliussen2016(
                rawdata=rawdata,
                target_classification=target_classification,
                dry_run=dry_run,
            )
        )
    except Exception:
        # Full traceback goes into Prefect logs
        logger.exception(
            "Preprocessing pipeline failed for this case — continuing with next case"
        )

    logger.info("# 3. Report generation")
    for _type in preprocessing.keys():
        try:
            logger.info(_type)
            reportgeneration_zarr(
                preprocessing=preprocessing[_type],
                target_classification=target_classification,
                bottom_detection=bottom_detection,
                cruise=cruise,
                reports=reports[_type],
                dry_run=dry_run,
            )
        except Exception:
            # Full traceback goes into Prefect logs
            logger.exception(
                "Preprocessing pipeline failed for this case — continuing with next case"
            )


def datacompression_flow(
    rawdata: Path,
    preprocessing: Path,
    quality_control: Path,
    dry_run: bool = False,
):
    """Two steps for the data compression"""
    logger.info("# 2a. Data compression: raw -> raw")
    korona_datacompression(
        rawdata=rawdata,
        preprocessing=preprocessing / Path("tmp"),
        quality_control=quality_control,
        dry_run=dry_run,
    )

    logger.info("# 2b. Data preprocessing: raw -> nc")
    korona_preprocessing(
        rawdata=preprocessing / Path("tmp"),
        preprocessing=preprocessing,
        dry_run=dry_run,
    )
