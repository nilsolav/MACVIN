import os
import zarr
import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import re

atc = xr.open_mfdataset(str(TARGET_CLASSIFICATION_2 / Path('*.nc')))
sv = xr.open_zarr(str(PREPROCESSING / Path(cruise+'_sv.zarr')))
report1 = xr.open_zarr(str(REPORTS_1_1 / Path(cruise+'_reports.zarr')))
report2 = xr.open_zarr(str(REPORTS_1_2 / Path(cruise+'_reports.zarr')))

time_intervals = [
    ("2005-11-09T05:50:17", "2005-11-09T05:57:37"),
    ("2005-11-09T04:50:02", "2005-11-09T04:53:30")
]
sliced_datasets = []
for start_time, end_time in time_intervals:
    # Slice the dataset using .sel()
    sliced_data = atc.sel(ping_time=slice(pd.Timestamp(start_time), pd.Timestamp(end_time)),
                          category=1000004)

.T.plot(ax=axs[1])
sliced_datasets.append(sliced_data)

# Now you can access the sliced datasets
for i, sliced_data in enumerate(sliced_datasets):
    print(f"Sliced Data for Interval {i+1}:")
    tmp = sliced_data.T.plot(ax=axs[i])

