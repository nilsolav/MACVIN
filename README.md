# MACVIN

**MACVIN** is a Python project built **uv** for orchestrating and running MACVIN data pipelines in a clean, reproducible way.

The project uses:

* **uv** for fast, reproducible dependency management
* **`src/` layout**

# Data organsisation
The list of cruises are described here:
[cruise list](cruises.csv)


| Column              | Description                                                   |
|---------------------|---------------------------------------------------------------|
| cruiseseries        | Keyword for the dta collection                                |
| Description         | Surey description                                             |
| status              | OK=Finalized, FAIL=Error with data, not salvagble             |
| year                | Year of cruise                                                |
| cruise              | Cruise ID                                                     |
| range               | Range in meters for automated processing                      |
| shipname            | Name of the vessel                                            |
| RAW_files           | The path to the raw acoustic data in EK60/80 format           |
| Original_RAW_files  | Path to the orginal raw files (typically EK500 files          |
| Original_WORK_files | Path to the orginal work files (if exist)                     |
| WS_WORK_files       | LSSS files generated at the workshop                          |


## Processed data

The processing flow reads the raw acoustic data and preprocess the data using three different steps, then applied Rolf's acosutic target classification (ATC) model. Finally the preprocessed data are combined with ATC annotations and the luf file is written.

These steps are 

| Processing step       | Name                      | Description                                                            |
|-----------------------|---------------------------|------------------------------------------------------------------------|
| preprocessing         | korona_noisefiltering     | Applies noise filtering on raw data and cnvert to netcdf               |
| preprocessing         | korona-preprocessing      | Conversion of raw data to netcdf                                       |
| preprocessing         | korona-datacompression    | Algorithm to compress raw used on Sounder and then converted to Netcdf |
| target-classification | mackerel-korneliussen2016 | ATC step                                                               |
| reportgeneration      | reportgeneration-zarr     | Integration step                                                       |


The processed data are stored under `$PROCESSED=/data/s3/MACWIN-scratch/silver/{$CRUISE}/`, where `$CRUISE` is 
the `cruise` column in [cruise list](cruises.csv).

The results for each task are located in:
```
/data/s3/MACWIN-scratch/silver/{$CRUISE}/
└── EK
    ├── PREPROCESSING
    │   ├── korona_datacompression
    │   ├── korona_noisefiltering
    │   └── korona_preprocessing
    ├── QUALITY_CONTROL
    │   └── korona_datacompression
    ├── REPORTS
    │   ├── korona_datacompression
    │   ├── korona_noisefiltering
    │   └── korona_preprocessing
    └── TARGET_CLASSIFICATION
        └── korona_noisefiltering
```

---

## 🚀 Features

* Reproducible environments via `uv.lock`
* Console scripts for running pipelines
* Clean separation of code, tests, and configuration

---

## 📁 Project structure

```
MACVIN/
├── pyproject.toml
├── uv.lock
├── README.md
├── src/
│   └── macvin/
│       ├── __init__.py
│       ├── pipeline.py
│       └── test.py
└── tests/
    └── test_pipeline.py
```


# Processing

To convert ek500 data the LSSS system must be installed.

The project requires acces to these docker containers:

```bash
acoustic-ek-reportgeneration-zarr:latest
acoustic-ek-target_classification-mackerel-korneliussen2016:latest
acoustic-ek_preprocessing_korona-noisefiltering_blueinsight:local
acoustic-ek_preprocessing_korona-preprocessing_blueinsight:local
acoustic-ek_preprocessing_korona-fixidx_blueinsight:local
```

## Acoustic ek containers

Build the noisefiltering, preprocessing, fixidx, and the mackerel classifier docker images:
```bash
git clone git@git.imr.no:crimac-wp4-machine-learning/CRIMAC-acoustic-ek
make build-all \
	docker-cleanup
```



---

## 🧰 Requirements

* Python **3.10**
* [`uv`](https://github.com/astral-sh/uv)

Install `uv` if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

---

## ⚙️ Setup

### 1️⃣ Clone the repository

```bash
git clone https://github.com/nilsolav/MACVIN
cd MACVIN
```

### 2️⃣ Sync the environment

```bash
uv sync
```

This installs:

* All dependencies
* The `macvin` package itself (editable install)

---

---

## ▶️ Running the pipeline

The order of the steps are:

* ek500conversion - converts from Ek500 to EK 60 data (if applicable)
* idxprocessing - Fix the index files for gaps
* preprocessing - Converts the EK60 files to NetCDF
* atcprocessing - Runs the Korona Mackerel classifier
* reports - Generates the reports from prepocessed files and the atc predictions
* lufreports - Convert the reports to xml files

Console scripts are used for data procesing. Flow runs will appear in the rotating `macvin.log` file.

Examples: If cruise is not given, all cruises will be processed, but note that cruises that are marked
as OK or FAIL in the status column in [cruise list](cruises.csv) will not be rerun.
```bash
uv run macvin-status --quick-run --cruise S1513S_PSCOTIA_MXHR6
uv run macvin-ek500conversion --dry-run --cruise S1513S_PSCOTIA_MXHR6
uv run macvin-idxprocessing --dry-run --cruise S1513S_PSCOTIA_MXHR6
uv run macvin-preprocessing --dry-run --cruise S1513S_PSCOTIA_MXHR6
uv run macvin-atcprocessing --dry-run --cruise S1513S_PSCOTIA_MXHR6
uv run macvin-reports --dry-run --cruise S1513S_PSCOTIA_MXHR6
uv run macvin-lufreports --dry-run --cruise S1513S_PSCOTIA_MXHR6
uv run macvin-status --quick-run
uv run macvin-ek500conversion --dry-run
uv run macvin-idxprocessing --dry-run
uv run macvin-preprocessing --dry-run
uv run macvin-atcprocessing --dry-run
uv run macvin-reports --dry-run
uv run macvin-lufreports --dry-run
uv run macvin-test
```

Use the dry run option for testing without running the docker steps:
```bash
uv run macvin-pipeline  --dry-run
```

### macvin-test

Run the pipeline on a test data set to test that the processing works:

```bash
uv run macvin-test
```

### macvin-status

Run this to get data and processing status:

```bash
uv run macvin-status
```

---

## 🧪 Running tests

```bash
uv run pytest
```

---

## 🛠 Development workflow

Common commands:

```bash
uv add <package>        # add dependency
uv add --dev <package> # add dev dependency
uv sync                # sync environment
uv run <command>       # run inside uv env
```

---

## 📦 Packaging details

This project uses a `src/` layout and is configured as a packaged project:

```toml
[tool.uv]
package = true
```

Console scripts are defined in `pyproject.toml`:

```toml
[project.scripts]
macvin-pipeline = "macvin.pipeline:main"
macvin-test = "macvin.test:main"
```

---


## 📖 Further reading

* uv documentation: [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv)

---

