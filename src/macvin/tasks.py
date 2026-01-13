from typing import Mapping
from pathlib import Path
import subprocess
import logging

logger = logging.getLogger(__name__)


def run_docker_image(
    image: str,
    volumes: Mapping[Path, str],
    artifact_key: str | None = None,
    env: Mapping[str, str] | None = None,
    dry_run: bool = False,
):

    command = ["docker", "run", "--rm"]

    for host_path, container_path in volumes.items():
        command.extend(["-v", f"{host_path}:{container_path}"])

    # Environment variables
    if env:
        for key, value in env.items():
            _env = ["-e", f"{key}={value}"]
            logger.info(_env)
            command.extend(_env)

    command.extend(
        [
            "--security-opt",
            "label=disable",
            image,
        ]
    )
    logger.debug(command)
    for host, container in volumes.items():
        art = f"Mapping : `{host}` → `{container}`"
        logger.info(art)

    if dry_run:
        logger.info("Dry run enabled – Docker command not executed")
        return

    try:
        logger.info("Try running Docker image: %s", image)
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.debug("Docker stdout:\n%s", result.stdout)
        logger.debug("Docker stderr:\n%s", result.stderr)

        logger.info("### Docker task completed successfully ###")

    except subprocess.CalledProcessError as e:
        logger.error("Docker failed with exit code %s", e.returncode)
        logger.error("Docker stdout:\n%s", e.stdout)
        logger.error("Docker stderr:\n%s", e.stderr)
        raise


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
        env=None,
        dry_run=dry_run,
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
        env=None,
        dry_run=dry_run,
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
        env=None,
        dry_run=dry_run,
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
        env=None,
        dry_run=dry_run,
    )


def reportgeneration_zarr(
    preprocessing: Path,
    target_classification: Path,
    bottom_detection: Path,
    cruise: str,
    reports: Path,
    dry_run: bool = False,
):
    env = {
        "CATEGORY": '["1000004", "1000005"]',
        "CRUISE": cruise,
    }

    return run_docker_image(
        image="acoustic-ek_reportgeneration-zarr:latest",
        volumes={
            preprocessing: "/PREPROCESSING",
            target_classification: "/TARGET_CLASSIFICATION",
            reports: "/REPORTS",
        },
        artifact_key="reportgeneration_zarr",
        env=env,
        dry_run=dry_run,
    )
