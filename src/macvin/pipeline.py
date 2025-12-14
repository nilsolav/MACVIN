import logging
from macvin.flows import macvin_full_flow

def main():
    macvin_full_flow()

"""
from pathlib import Path
from typing import Mapping
import subprocess
import datetime
import pandas as pd
import os
import logging
from prefect.futures import wait

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    macvin_full_flow()



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
