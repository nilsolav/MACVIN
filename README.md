# MACVIN
Processing scripts for the MACVIN project

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

# Processing


## Korona processing script

Build first the mackerel classifier into a docker image:
```bash
git clone git@git.imr.no:crimac-wp4-machine-learning/CRIMAC-acoustic-target-classification.git
cd docker/mackerel-korneliussen2016
docker build --build-arg=commit_sha=$(git rev-parse HEAD) --build-arg=version_number=$(git describe --tags) --tag acoustic-ek-target_classification-mackerel-korneliussen2016 .
```

Then run
```bash
python runmackerelclassifier.py
```
to loop over all surveys that have EK60 or EK80 raw and run the Korona classifier. The Korona processing 
script is set up in python and uses KoronaScript to run korona and batch proccesses the raw data.

## Report generation
Build first the report generator as a docker image:
```bash
git clone git@git.imr.no:crimac-wp4-machine-learning/CRIMAC-reportgeneration-zarr.git
docker build --build-arg=commit_sha=$(git rev-parse HEAD) --build-arg=version_number=$(git describe --tags) --tag acoustic-ek-reportgeneration-zarr .
```

Then run
```bash
python runreports.py
```
to loop over all surveys to generate the reports. Current setup provides minimal version of the luf26 for StoX and a 
Zarr store containing all frequencies.
