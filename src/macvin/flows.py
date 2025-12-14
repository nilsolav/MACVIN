from prefect import flow
from pathlib import Path
from macvin.tasks import (
    korona_noisefiltering,
    korona_preprocessing,
    korona_datacompression,
    mackerel_korneliussen2016,
    reportgeneration_zarr,
)
import os
import pandas as pd
from prefect.logging import get_run_logger


@flow(name="macvin_full_flow")
def macvin_full_flow():
    logger = get_run_logger()
    logger.info(f"#### MACVIN FULL FLOW ####")

    # f = '/home/nilsolav/repos/MACVIN/cruises.csv'
    # df = pd.read_csv(f)
    df = pd.read_csv("cruises.csv")

    basedir = Path("/s3")

    # for _cr, i in (cr['RAW_files']):

    for idx, row in df.iterrows():
        cruise = row["cruise"]
        bronze_dir = row["RAW_files"]

        logger.info(cruise)

        logger.info(bronze_dir)
        silver_dir = (
            basedir / Path("test_data_azure_silver") / cruise / Path("ACOUSTIC", "EK")
        )
        logger.info(silver_dir)
        # survey_flow(survey_id = str(cruise), bronze_dir = bronze_dir,
        #            silver_dir = silver_dir, dry_run = False)


@flow(name="macvin_test_flow")
def macvin_test_flow():
    basedir = Path(os.getenv("CRIMACSCRATCH"))
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
        survey_id=str(cruise),
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        dry_run=True,
    )


@flow(name="survey_flow")
def survey_flow(
    survey_id: str,
    bronze_dir: Path,
    silver_dir: Path,
    dry_run: bool = False,
):
    logger = get_run_logger()
    logger.info(f"#### {survey_id} ####")
    rawdata = bronze_dir
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
            "mackerel_korneliussen2016" "reportgeneration-zarr",
        ),
        "datacompression": silver_dir
        / Path(
            "REPORTS",
            "korona_datacompression",
            "mackerel_korneliussen2016",
            "reportgeneration-zarr",
        ),
    }

    futures = []
    logger.info("# 1a. Noise filtering")
    futures.append(
        korona_noisefiltering(
            rawdata=rawdata,
            preprocessing=preprocessing["noisefiltering"],
            dry_run=dry_run,
        )
    )

    logger.info("# 1b. Data compression (use a sub flow since it is 2 steps")
    futures.append(
        datacompression_flow(
            rawdata=rawdata,
            preprocessing=preprocessing["datacompression"],
            quality_control=quality_control,
            dry_run=dry_run,
        )
    )

    logger.info("# 1c. Preprocesing")
    futures.append(
        korona_preprocessing(
            rawdata=rawdata,
            preprocessing=preprocessing["preprocessing"],
            dry_run=dry_run,
        )
    )

    logger.info("# 2. Target classification")
    futures.append(
        mackerel_korneliussen2016(
            rawdata=rawdata,
            target_classification=target_classification,
        )
    )

    logger.info("# 3. Report generation")
    for _type in preprocessing.keys():
        logger.info(_type)
        reportgeneration_zarr(
            preprocessing=preprocessing[_type],
            target_classification=target_classification,
            bottom_detection=bottom_detection,
            reports=reports[_type],
        )


@flow(name="datacompression_flow")
def datacompression_flow(
    rawdata: Path,
    preprocessing: Path,
    quality_control: Path,
    dry_run: bool = False,
):
    logger = get_run_logger()

    """Two steps for the data compression"""
    logger.info("# 2a. Data compression: raw -> raw")
    korona_datacompression(
        rawdata=rawdata,
        preprocessing=preprocessing / Path("tmp"),
        quality_control=quality_control,
    )

    logger.info("# 2b. Data preprocessing: raw -> nc")
    korona_preprocessing(
        rawdata=preprocessing / Path("tmp"),
        preprocessing=preprocessing,
    )
