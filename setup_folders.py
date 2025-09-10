import os
import pandas as pd

# List of survey
df = pd.read_csv('cruises.csv', sep=';')

root = '/data/s3/MACWIN-scratch/'

# Create folders for raw data (only scottish data since imr data are on cruise data s3)
for index, row in df.iterrows():
    if row['shipname'] in ['Scotia', 'Sunbeam']:
        cruise = row['cruise']
        year = row['year']
        # List of survey names
        raw = os.path.join(root, str(year), cruise, 'ACOUSTIC', 'EK60', 'EK60_RAWDATA')
        print(raw)
        os.makedirs(raw, exist_ok=True)
    
# This is the folder for the Korona files from the workshop    
for index, row in df.iterrows():
    cruise = row['cruise']
    year = row['year']
    korona = os.path.join(root, str(year), cruise, 'ACOUSTIC', 'LSSS', 'WORK')
    print(korona)
    os.makedirs(korona, exist_ok=True)

# This is the folders for the output from the Korona procesing
for index, row in df.iterrows():
    cruise = row['cruise']
    year = row['year']
    korona = os.path.join(root, str(year), cruise, 'ACOUSTIC', 'LSSS', 'KORONA')
    print(korona)
    os.makedirs(korona, exist_ok=True)
