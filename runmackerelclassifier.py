from pathlib import Path
import os
import pandas as pd
import subprocess

cr = pd.read_csv('cruises.csv')

for i, _row in cr.iterrows(): 
    rawdatapath = Path(_row['RAW_files'])
    cruise = _row['cruise']
    
    if rawdatapath.exists():
        for filename in os.listdir(rawdatapath):
            if filename.endswith('.raw'):
                rawfileexist = True
            else:
                rawfileexist = False
    else:
        rawfileexist = False
    print(rawfileexist)
    
    if rawfileexist:
        target_classification = Path(_row['koronaprocessing'])

        # Create directory if not existing
        target_classification.mkdir(parents=True, exist_ok=True)

        command = [
            "docker", "run", "-it", "--rm",
            "-v", str(rawdatapath)+':/RAWDATA',
            "-v", str(target_classification)+':/TARGET_CLASSIFICATION',
            "--security-opt", "label=disable",
            "--env", "OUTPUT_TYPE=nc",
            "--env", "OUTPUT_NAME="+cruise,
            "acoustic-ek-target_classification-mackerel-korneliussen2016:latest"]

        print(cruise)
        print(' ')
        print(command)
        print(' ')
        # Run the command
        try:
            subprocess.run(command, check=True)
            print('Successfully processed')
        except subprocess.CalledProcessError as e:
            print(f"Command failed with error: {e}")
