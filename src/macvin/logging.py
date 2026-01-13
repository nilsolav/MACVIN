import logging
import logging.handlers
import sys
from pathlib import Path


def setup_logging(
    name: str = None,
    log_file: Path | str = "app.log",
    level: int = logging.DEBUG,
):
    """
    Configure logging with:
      - stdout handler (<= INFO)
      - stderr handler (>= WARNING)
      - rotating file handler (DEBUG+)

    Returns a configured logger.

    DEBUG < INFO < WARNING < ERROR < CRITICAL
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # prevent double logging

    if logger.handlers:
        return logger  # already configured

    log_file = Path(log_file)

    # ---- Formatter ----
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ---- stdout handler (INFO and below) ----
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    stdout_handler.setFormatter(formatter)

    # ---- stderr handler (WARNING and above) ----
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(formatter)

    # ---- rotating file handler (everything) ----
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # ---- attach handlers ----
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    logger.addHandler(file_handler)

    return logger
