from prefect import task
from typing import Mapping
from pathlib import Path
import subprocess
from prefect.logging import get_run_logger


@task
def run_docker_image(
    image: str,
    volumes: Mapping[Path, str],
    artifact_key: str | None = None,
    dry_run: bool = False,
):

    logger = get_run_logger()
    
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

    if not dry_run: 
        subprocess.run(command, check=True)

    # ---- Artifact ----
    art = "\n".join(f"- `{host}` → `{container}`" for host, container in volumes.items())
    logger.info(art)
    logger.info(" Docker task completed successfully ###")


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

