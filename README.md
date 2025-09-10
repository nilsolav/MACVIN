# MACVIN
Processing scripts for the MACVIN project

# Data organsisation
The list of cruises are described here:
[cruise list](cruises.csv)

| Column | Description |
|---|---|
|cruiseseries | Keyword for the dta collection |
| year | Year of cruise |
| cruise | ID composed of 'S' + Cruise code + '_P' + Platform name + '_' + Call sign
| range| Range in meters for automated processing |
| shipname | Name of the vessel |
| RAW_files | The path to the raw acoustic data |
| Original_WORK_files | Path to the orginal work files (if exist) |
| WS_WORK_files	| LSSS files generated at the workshop |
| koronaprocessing | Path to the predictions from the Korona categorization module |

# Processing

## LSSS

## Korona processing script

The Korona processing script is set up in python and uses KoronaScript to run korona and batch proccesses the raw data.


