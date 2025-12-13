from prefect import task, flow
#from prefect.logging import get_run_logger
#from prefect.artifacts import create_markdown_artifact
from pathlib import Path
from typing import Mapping
import subprocess
import datetime
import pandas as pd
import os
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

logger.info("This is an info message")
logger.warning("This is a warning")


@flow(name="survey_flow")
def survey_flow(
    survey_id: str,
    bronze_dir: Path,
    silver_dir: Path,
    dry_run: bool = False,
):

    rawdata = bronze_dir / Path("EK_RAWDATA")
    preprocessing = {
        "noisefiltering": silver_dir
        / Path(
            "PREPROCESSING",
            "korona_noisefiltering"
        ),
        "preprocessing": silver_dir
        / Path(
            "PREPROCESSING",
            "korona_preprocessing"
        ),
        "datacompression": silver_dir
        / Path(
            "PREPROCESSING",
            "korona_datacompression"
        ),
    }

    target_classification = silver_dir / Path(
            "TARGET_CLASSIFICATION",
            "korona_noisefiltering",
        "mackerel_korneliussen2016",
    )

    quality_control = silver_dir / Path(
        "QUALITY_CONTROL", "korona_datacompression")
    bottom_detection = None
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
            "mackerel_korneliussen2016"
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

    logger.info('# 1a. Noise filtering')
    korona_noisefiltering(
        rawdata=rawdata,
        preprocessing=preprocessing["noisefiltering"],
        dry_run=dry_run,
    )

    logger.info('# 1b. Data compression (use a sub flow since it is 2 steps')
    datacompression_flow(
        rawdata=rawdata,
        preprocessing=preprocessing["datacompression"],
        quality_control=quality_control,
        dry_run=dry_run,
    )

    logger.info('# 1c. Preprocesing')
    korona_preprocessing(
        rawdata=rawdata,
        preprocessing=preprocessing["preprocessing"],
        dry_run=dry_run,
    )
    
    logger.info('# 2. Target classification')
    mackerel_korneliussen2016(
        rawdata=rawdata,
        target_classification=target_classification,
    )
    
    logger.info('# 3. Report generation')
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
    """Two steps for the data compression"""
    # 2a. Data compression: raw -> raw
    korona_datacompression(
        rawdata=rawdata,
        preprocessing=preprocessing / Path("tmp"),
        quality_control=quality_control,
    )

    # 2b. Data preprocessing: raw -> nc
    korona_preprocessing(
        rawdata=preprocessing / Path("tmp"),
        preprocessing=preprocessing,
    )


@task
def run_docker_image(
    image: str,
    volumes: Mapping[Path, str],
    artifact_key: str | None = None,
    dry_run: bool = False,
):
    #logger = get_run_logger()

    command = ["docker", "run", "--rm"]

    for host_path, container_path in volumes.items():
        command.extend(["-v", f"{host_path}:{container_path}"])

    command.extend(
        [
            "--security-opt",
            "label=disable",
            image,
        ]
    )

    logger.info("Running Docker image: %s", image)
    logger.debug("Command: %s", " ".join(command))

    start_time = datetime.datetime.utcnow()
    if not dry_run: 
        subprocess.run(command, check=True)

    end_time = datetime.datetime.utcnow()

    # ---- Artifact ----
    if artifact_key:
        volume_table = "\n".join(
            f"- `{host}` → `{container}`" for host, container in volumes.items()
        )

        markdown = f"### Docker task completed successfully ###"


def korona_noisefiltering(
    rawdata: Path,
    preprocessing: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_preprocessing_korona-noisefiltering_blueinsight:local",
        volumes={
            rawdata: "/RAWDATA",
            preprocessing: "/PREPROCESSING",
        },
        artifact_key="korona-noisefiltering",
    )


def korona_preprocessing(
    rawdata: Path,
    preprocessing: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_preprocessing_korona-preprocessing_blueinsight:local",
        volumes={
            rawdata: "/RAWDATA",
            preprocessing: "/PREPROCESSING",
        },
        artifact_key="korona-preprocessing",
    )


def korona_datacompression(
    rawdata: Path,
    preprocessing: Path,
    quality_control: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_preprocessing_korona-datacompression_blueinsight:local",
        volumes={
            rawdata: "/RAWDATA",
            preprocessing: "/PREPROCESSING",
            quality_control: "/QUALITY_CONTROL",
        },
        artifact_key="korona-datacompression",
    )


def mackerel_korneliussen2016(
    rawdata: Path,
    target_classification: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_target-classification_mackerel-korneliussen2016_blueinsight:local",
        volumes={
            rawdata: "/RAWDATA",
            target_classification: "/TARGET_CLASSIFICATION",
        },
        artifact_key="mackerel_korneliussen2016",
    )


def reportgeneration_zarr(
    preprocessing: Path,
    target_classification: Path,
    bottom_detection: Path,
    reports: Path,
    dry_run: bool = False,
):
    
    return run_docker_image(
        image="acoustic-ek_reportgeneration-zarr:latest",
        volumes={
            preprocessing: "/PREPROCESSING",
            target_classification: "/TARGET_CLASSIFICATION",
            reports: "/REPORTS",
        },
        artifact_key="reportgeneration_zarr",
    )



def macvin_flow():
    basedir = Path(os.getenv("CRIMACSCRATCH"))
    cr = pd.read_csv("cruises.csv")
    cruise = Path("S2005114_PGOSARS_4174")
    bronze_dir = basedir / Path("test_data_azure") / cruise / Path("ACOUSTIC", "EK")
    silver_dir = basedir / Path("test_data_azure_silver") / cruise / Path("ACOUSTIC", "EK")
     
    survey_flow(survey_id = str(cruise), bronze_dir = bronze_dir, silver_dir = silver_dir, dry_run=True)


def main():
    macvin_flow()


"""
for i, _row in cr.iterrows(): 
    rawdatapath = Path(_row['RAW_files'])
    cruise = _row['cruise']
    print(cruise)

        rawdata =   basedir / Path('EK_RAWDATA')


if __name__ == "__main__":
    korona_processing_flow(
        rawdata=Path("/data/raw"),
        preprocessing=Path("/data/preprocessing"),
        quality_control=Path("/data/qc"),
        target_classification=Path("/data/targets"),
        bottom_detection=Path("/data/bottom"),
        reports=Path("/data/reports"),
    )
"""
