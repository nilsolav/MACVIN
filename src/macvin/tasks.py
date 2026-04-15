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
        logger.info("Docker image %s completed successfully", image)

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
        image="acoustic-ek_preprocessing_korona-fixidx:local",
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
        image="acoustic-ek_preprocessing_korona-noisefiltering:local",
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
        image="acoustic-ek_preprocessing_korona-preprocessing:local",
        volumes={
            "/RAWDATA": str(rawdata),
            "/IDX": str(idxdata),
            "/PREPROCESSING": str(preprocessing),
        },
        artifact_key="korona-preprocessing",
        env=None,
        dry_run=dry_run,
    )


def mackerel_korneliussen2016(
    preprocessing: Path,
    target_classification: Path,
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_target-classification_mackerel-korneliussen2016:local",
        volumes={
            "/PREPROCESSING": str(preprocessing),
            "/TARGET_CLASSIFICATION": str(target_classification),
        },
        artifact_key="mackerel_korneliussen2016",
        env=None,
        dry_run=dry_run,
    )


def sv_echo_integrator(
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
        "SEABED_REMOVE": True, # Change to True after testing
        "SEABED_PAD": -0.5,
        "SURFACE_PAD": 0,
        "SV_LOWER_THRESHOLD": -82, # false or dB
        "PingAxisInterval": 0.1,
        "ChannelDepthInterval": 10
    }

    return run_docker_image(
        image="acoustic-ek_reports_sv-echo-integrator:local",
        volumes={
            "/PREPROCESSING": str(preprocessing),
            "/TARGET_CLASSIFICATION": str(target_classification),
            "/REPORTS": str(reports),
        },
        artifact_key="reportgeneration_zarr",
        env=env,
        dry_run=dry_run,
    )
