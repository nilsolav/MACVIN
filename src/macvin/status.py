from prefect import flow
from pathlib import Path
import pandas as pd
from prefect.logging import get_run_logger


@flow(name="macvin_get_status")
def macvin_get_status():
    
    logger = get_run_logger()
    logger.info("#### MACVIN STATUS FLOW ####")

    df = pd.read_csv("cruises.csv")

    basedir = Path("/data/s3/MACWIN-scratch")
    
    for idx, row in df.iterrows():
        cruise = row["cruise"]
        silver_dir = (
            basedir / Path("silver") / cruise / Path("ACOUSTIC", "EK")
        )
        survey_status(silver_dir, logger, cruise)


def survey_status(silver_dir: Path, logger, cruise):
    # report
    report = silver_dir / Path("REPORTS", 
                               "korona_noisefiltering",
                               "mackerel_korneliussen2016",
                               "reportgeneration-zarr",
                               "ListUserFile26_.xml",
                               )

    # sv_nc
    sv_dir = silver_dir / Path("PREPROCESSING",
                               "korona_preprocessing",
                               )
    sv_nc = len(list(sv_dir.glob("*.nc")))

    # labels_nc
    labels_dir = silver_dir / Path("TARGET_CLASSIFICATION",
                                   "korona_noisefiltering",
                                   "mackerel_korneliussen2016",
                                   )
    labels_nc = len(list(labels_dir.glob("*.nc")))
    
    logger.info(f"Cruise: {cruise}, sv_nc: {sv_nc}, labels_nc: {labels_nc}, luf exist: {report.exists()}")


def main():
    macvin_get_status()
