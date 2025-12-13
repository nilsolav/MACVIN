# MACVIN

**MACVIN** is a Python project built with **Prefect 3** and **uv** for orchestrating and running MACVIN data pipelines in a clean, reproducible way.

The project uses:

* **Prefect 3** for workflow orchestration
* **uv** for fast, reproducible dependency management
* A modern **`src/` layout**
* Optional integration with **Prefect Cloud**

# Data organsisation
The list of cruises are described here:
[cruise list](cruises.csv)

| Column              | Description                                                               |
|---------------------|---------------------------------------------------------------------------|
| cruiseseries        | Keyword for the dta collection                                            |
| year                | Year of cruise                                                            |
| cruise              | ID composed of 'S' + Cruise code + '_P' + Platform name + '_' + Call sign |
| range               | Range in meters for automated processing                                  |
| shipname            | Name of the vessel                                                        |
| RAW_files           | The path to the raw acoustic data                                         |
| Original_WORK_files | Path to the orginal work files (if exist)                                 |
| WS_WORK_files       | LSSS files generated at the workshop                                      |
| koronaprocessing    | Path to the predictions from the Korona categorization module             |



---

## 🚀 Features

* Prefect 3 flows and tasks
* Cloud-first orchestration (no local server required)
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

The project requires acces to dockerized steps.


## Korona processing script

Build first the mackerel classifier into a docker image:
```bash
```

## Report generation
Build first the report generator as a docker image:
```bash
git clone git@git.imr.no:crimac-wp4-machine-learning/CRIMAC-reportgeneration-zarr.git
docker build --build-arg=commit_sha=$(git rev-parse HEAD) --build-arg=version_number=$(git describe --tags) --tag acoustic-ek-reportgeneration-zarr .
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
git clone <your-repo-url>
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

## 🔐 Prefect Cloud authentication

This project is designed to run with **Prefect Cloud**.

Authenticate once:

```bash
uvx prefect-cloud login --key <YOUR_API_KEY>
```

Your API key is stored securely in your local Prefect profile
(`~/.prefect/profiles.toml`) and is **not committed to git**.

Verify:

```bash
uv run prefect profile ls
```

---

## ▶️ Running the pipeline

The main pipeline is exposed as a console script.

```bash
uv run macvin-pipeline
```

You can also run it as a module:

```bash
uv run python -m macvin.pipeline
```

Flow runs will appear in the Prefect Cloud UI.

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

## ☁️ Prefect deployments (optional)

To deploy the flow to Prefect Cloud:

```bash
uv run prefect work-pool create macvin-pool --type process
uv run prefect worker start --pool macvin-pool
uv run prefect deploy src/macvin/pipeline.py:macvin_flow \
  --name macvin-local \
  --pool macvin-pool
```

---

## 🔒 Notes on security

* API keys are **never stored in code**
* Secrets used by flows should be stored using **Prefect Secret blocks**
* `.env` files (if used) should be added to `.gitignore`

---

## 📖 Further reading

* Prefect documentation: [https://docs.prefect.io](https://docs.prefect.io)
* uv documentation: [https://github.com/astral-sh/uv](https://github.com/astral-sh/uv)

---

