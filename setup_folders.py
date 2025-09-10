import os
import pandas as pd

# List of survey
df = pd.read_csv('cruises.csv', sep=';')

# Create folders
for index, row in df.iterrows():
    print(row)
    print(index)
    cruise = row['cruise']
    year = row['year']
    # List of survey names
    raw = os.path.join(str(year), cruise, 'ACOUSTIC', 'EK80', 'EK80_RAWDATA')
    os.makedirs(raw, exist_ok=True)
    
    korona = os.path.join(str(year), cruise, 'ACOUSTIC', 'LSSS', 'KORONA')
    os.makedirs(korona, exist_ok=True)

