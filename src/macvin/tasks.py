from typing import Mapping
from pathlib import Path
import subprocess
import logging
from zarr2lufxml import write_acoustic_xml
import xarray as xr
import threading
from collections.abc import Mapping


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
        logger.info(f"The content of the reports.zarr store:\n {zr}")
        cat = par["category"]
        logger.info(cat)
        logger.info(par["frequency"])
        #if "all" not in cat:
        #    zr = zr.assign_coords(category=[str(c) for c in zr.category.values])
        zr = zr.sel(category=cat, frequency=par["frequency"])

        logger.info(zr)

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
        logger.debug("Mount: %s:%s", host_path, container_path)

    if env:
        for key, value in env.items():
            command.extend(["-e", f"{key}={value}"])
            logger.debug("Env: %s=%s", key, value)

    command.extend(
        [
            "--security-opt",
            "label=disable",
            image,
        ]
    )

    logger.debug("Docker command: %s", command)

    if dry_run:
        logger.info("Dry run enabled – Docker command not executed")
        return

    logger.info("Running Docker image: %s", image)

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,  # line-buffered
    )

    stdout_thread = threading.Thread(
        target=_stream_pipe,
        args=(process.stdout, logger.info),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_stream_pipe,
        args=(process.stderr, logger.warning),
        daemon=True,
    )

    stdout_thread.start()
    stderr_thread.start()

    return_code = process.wait()

    stdout_thread.join()
    stderr_thread.join()

    if return_code != 0:
        logger.error("Docker failed with exit code %s", return_code)
        raise subprocess.CalledProcessError(return_code, command)

    logger.info("Docker image %s completed successfully", image)


def korona_fixidx(
    idx: Path,  # Location of idx+raw
    preprocessing: Path,  # Location of updated idx files
    dry_run: bool = False,
):
    return run_docker_image(
        image="acoustic-ek_processing_korona-fixidx:local",
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
        image="acoustic-ek_processing_korona-noisefiltering:local",
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
        image="acoustic-ek_processing_korona-preprocessing:local",
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


def atc2zarr(
    nc_mount: Path,
    zarr_mount: Path,
    cruise: str,
    dry_run: bool = False,
):
    env = {
        "ZARR_STORE": "labels.zarr",
        "CHUNK_SIZE_PING_TIME": 1024,
        "CHUNK_SIZE_RANGE": 2500,
        "CHUNK_SIZE_FREQUENCY": 1,
        "CHUNK_SIZE_CATEGORY": 1,
    }

    return run_docker_image(
        image="acoustic-ek_processing_nc-zarr:local",
        volumes={
            "/NC_MOUNT": str(nc_mount),
            "/ZARR_MOUNT": str(zarr_mount),
        },
        artifact_key="nc_zarr",
        env=env,
        dry_run=dry_run,
    )


def preprocess2zarr(
    nc_mount: Path,
    zarr_mount: Path,
    cruise: str,
    dry_run: bool = False,
):
    env = {
        "ZARR_STORE": "sv.zarr",
    }

    return run_docker_image(
        image="acoustic-ek_processing_nc-zarr:local",
        volumes={
            "/NC_MOUNT": str(nc_mount),
            "/ZARR_MOUNT": str(zarr_mount),
        },
        artifact_key="nc_zarr",
        env=env,
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
        "CATEGORIES": '["1000004"]',
        "FREQUENCIES": '["200000"]',
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


def _stream_pipe(pipe, log_func):
    """Stream a subprocess pipe line-by-line into logger."""
    try:
        for line in iter(pipe.readline, ""):
            if line:
                log_func(line.rstrip())
    finally:
        pipe.close()


