from pathlib import Path
import os
import pandas as pd
import subprocess

cr = pd.read_csv('cruises.csv')

'''
Standard data mounting directories
/RAWDATA          : Location of the raw files
/WORK             : Location of the LSSS work files
/PREPROCESSING    : Location of the preprocessed  <survey>_sv.zarr and 
                    <survey>_labels.zarr zarr stores or the folder of 
                    the preprocessed files in netcdf format
/BOTTOM_DETECTION : Location of the <survey>_bottom.zarr files
/TARGET_CLASSIFICATION : Lcation of the acoustic target classification files
/REPORTS          : Location of the report files
'''

for i, _row in cr.iterrows(): 
    rawdatapath = Path(_row['RAW_files'])
    cruise = _row['cruise']
    print(cruise)
    print('Rawpath:'+str(rawdatapath.exists()))
    if rawdatapath.exists():
        rawfileexist = False
        for filename in os.listdir(rawdatapath):
            if filename.endswith('.raw'):
                rawfileexist = True
                
        print('Rawfiles:'+str(rawfileexist))
    else:
        rawfileexist = False
    
    if rawfileexist:
        target_classification = Path(_row['koronaprocessing'])/Path('categorization')
        sv = Path(_row['koronaprocessing'])/Path('netcdf')
        report_path = target_classification.parent.parent.parent/Path(
            'EK80', 'REPORTS',
            'crimac-preprocessing-korona_mackerel-korneliussen2016')
        # Create directory if not existing
        #report_path.mkdir(parents=True, exist_ok=True)
        env = ({
            "CATEGORY": '["1000004", "1000005"]',
            "THRESHOLD": "0.75",
            "CRUISE": cruise,
        })

        command = [
            "docker", "run", "-it", "--rm",
            "-v", str(sv)+':/PREPROCESSING',
            "-v", str(target_classification)+':/TARGET_CLASSIFICATION',
            "-v", str(report_path)+':/REPORTS',
            "--security-opt", "label=disable",
        ]
        for key, value in env.items():
            command += ["--env", f"{key}={value}"]
        command.append("acoustic-ek-reportgeneration-zarr:latest")
        print(command)
        
        # Run the command
        try:
            subprocess.run(command, check=True)
            print('Successfully processed')
        except subprocess.CalledProcessError as e:
            print(f"Command failed with error: {e}")

