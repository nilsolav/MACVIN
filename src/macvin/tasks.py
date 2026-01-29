from typing import Mapping
from pathlib import Path
import subprocess
import logging
from zarr2lufxml import write_acoustic_xml
import xarray as xr

logger = logging.getLogger(__name__)


def run_zarr2lufxml(
    zarr_report: Path,
    luf_report: Path,
    par: dict,
    dry_run: bool = False,
):
    if not dry_run:
        _luf_report = str(luf_report)
        zr = xr.open_zarr(str(zarr_report))
        logger.debug(f"The content of the reports.zarr store:\n {zr}")
        cat = par["category"]
        if "all" not in cat:
            zr = zr.assign_coords(category=[str(c) for c in zr.category.values])
            zr = zr.sel(category=cat, frequency=par["frequency"])

        write_acoustic_xml(zr, par, _luf_report)


def run_docker_image(
    image: str,
    volumes: dict[str, str],
    artifact_key: str | None = None,
    env: Mapping[str, str] | None = None,
    dry_run: bool = False,
):
    command = ["docker", "run", "--rm"]

    for container_path, host_path in volumes.items():
        command.extend(["-v", f"{host_path}:{container_path}"])
        logger.debug(f"Mounts: {host_path}:{container_path}")
    # Environment variables
    if env:
        for key, value in env.items():
            _env = ["-e", f"{key}={value}"]
            logger.debug(_env)
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
        logger.debug(art)

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


def korona_fixidx(
    idx: Path,  # Location of idx+raw
    preprocessing: Path,  # Location of updated idx files
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_preprocessing_korona-fixidx_blueinsight:local",
        volumes={
            "/IDX": str(idx),
            "/PREPROCESSING": str(preprocessing),
        },
        artifact_key="korona-fixidx",
        env=None,
        dry_run=dry_run,
    )


def korona_noisefiltering(
    idxdata: Path,
    rawdata: Path,
    preprocessing: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_preprocessing_korona-noisefiltering_blueinsight:local",
        volumes={
            "/RAWDATA": str(rawdata),
            "/IDX": str(idxdata),
            "/PREPROCESSING": str(preprocessing),
        },
        artifact_key="korona-noisefiltering",
        env=None,
        dry_run=dry_run,
    )


def korona_preprocessing(
    idxdata: Path,
    rawdata: Path,
    preprocessing: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_preprocessing_korona-preprocessing_blueinsight:local",
        volumes={
            "/RAWDATA": str(rawdata),
            "/IDX": str(idxdata),
            "/PREPROCESSING": str(preprocessing),
        },
        artifact_key="korona-preprocessing",
        env=None,
        dry_run=dry_run,
    )


def korona_datacompression(
    idxdata: Path,
    rawdata: Path,
    preprocessing: Path,
    quality_control: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_preprocessing_korona-datacompression_blueinsight:local",
        volumes={
            "/RAWDATA": str(rawdata),
            "/IDX": str(idxdata),
            "/PREPROCESSING": str(preprocessing),
            "/QUALITY_CONTROL": str(quality_control),
        },
        artifact_key="korona-datacompression",
        env=None,
        dry_run=dry_run,
    )


def mackerel_korneliussen2016(
    idxdata: Path,
    rawdata: Path,
    target_classification: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_target-classification_mackerel-korneliussen2016_blueinsight:local",
        volumes={
            "/RAWDATA": str(rawdata),
            "/IDX": str(idxdata),
            "/TARGET_CLASSIFICATION": str(target_classification),
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
            "/PREPROCESSING": str(preprocessing),
            "/TARGET_CLASSIFICATION": str(target_classification),
            "/REPORTS": str(reports),
        },
        artifact_key="reportgeneration_zarr",
        env=env,
        dry_run=dry_run,
    )
