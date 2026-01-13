from pathlib import Path
import pandas as pd

# Load cruise table
cr = pd.read_csv("cruises.csv")


def count_raw_files(path: Path, suffix: str = ".raw") -> int:
    """Return the number of .raw files in a directory (0 if none or path missing)."""
    if not path.exists() or not path.is_dir():
        return 0
    return sum(1 for p in path.iterdir() if p.suffix == suffix)


# ---------------------------------------------------------------------
# Standard data directories (informative comment preserved)
# /RAWDATA
# /WORK
# /PREPROCESSING
# /BOTTOM_DETECTION
# /TARGET_CLASSIFICATION
# /REPORTS
# ---------------------------------------------------------------------

for _, row in cr.iterrows():
    cruise = row["cruise"]
    print("\n" + cruise)
    # Raw data
    rawdatapath = Path(row["RAW_files"])
    if rawdatapath.exists():
        rawfilecount = count_raw_files(rawdatapath, suffix=".raw")
        print(f"{rawdatapath} path exist and contains {rawfilecount} raw files.")
        rawfileexist = rawfilecount > 0
    else:
        rawfileexist = False
        print(f"Rawpath does not exist.")

    if not rawfileexist:
        continue

    reports = Path(row["koronaprocessing"])
    if target_classification.exists():
        atcfilecount = count_raw_files(target_classification, suffix=".nc")
        print(
            f"{target_classification} path exist and contains {atcfilecount} nc files."
        )
        atcfileexist = rawfilecount > 0
    else:
        atcfileexist = False
        print(f"ATC path does not exist.")

    target_classification = Path(row["koronaprocessing"])
    if target_classification.exists():
        atcfilecount = count_raw_files(target_classification, suffix=".nc")
        print(
            f"{target_classification} path exist and contains {atcfilecount} nc files."
        )
        atcfileexist = rawfilecount > 0
    else:
        atcfileexist = False
        print(f"ATC path does not exist.")

    """
    # Build report path more transparently
    report_path = (
        base_processing
        .parents[2]    # go three levels up
        / "EK80"
        / "REPORTS"
        / "crimac-preprocessing-korona_mackerel-korneliussen2016"
    )

    print(f"Reports: {target_classification}")

    # List .nc files in the report directory
    for ncfile in report_path.glob("*.nc"):
        print(ncfile)
    """
