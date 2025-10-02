from pathlib import Path
import os
import xarray as xr
import zarr
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

crimac_scratch = os.getenv('CRIMACSCRATCH')
survey = 'S2005114_PGOSARS_4174'

# For test data
dr = {}
dr['raw'] = Path('/data/cruise_data/2012/S2012843_PBRENNHOLM_4405/EK60Raw')

# From docker
dr['atc_docker'] = str(Path(crimac_scratch) / Path(
    'test_data_out') / Path(survey) / Path(
        'ACOUSTIC/TARGET_CLASSIFICATION/mackerel-korneliussen2016/'))+'/S2005114_PGOSARS_4174_labels.zarr'

# CRIMAC grid
dr['gridded'] = Path(crimac_scratch) / Path('test_data_out') / Path(
    survey) / Path('ACOUSTIC/GRIDDED')
dr['atc_work'] =  str(dr['gridded'])+'/S2005114_PGOSARS_4174_labels.zarr'
dr['sv'] =  str(dr['gridded'])+'/S2005114_PGOSARS_4174_sv.zarr'

# Korona
dr['atc_korona'] = (
    '/crimac-scratch/test_data/S2005114_PGOSARS_4174/ACOUSTIC/TARGET_CLASSIFICATION/'
    'acoustic-ek-target_classification-korona_korneliussen2016_korona-delayedmode/'
    'categorization/*.nc'
)

# From Rolf
dr['atc_rolf'] = (
    '/crimac-scratch/test_data/S2005114_PGOSARS_4174/ACOUSTIC/TARGET_CLASSIFICATION/'
    'acoustic-ek-target_classification-korona_korneliussen2016_rolf-delayedmode/'
    'categorization/*.nc')

atc_korona = xr.open_mfdataset(dr['atc_korona'], combine='by_coords')
atc_rolf = xr.open_mfdataset(dr['atc_rolf'], combine='by_coords')
atc_docker = xr.open_zarr(dr['atc_docker'], chunks='auto')
atc_work = xr.open_zarr(dr['atc_work'], chunks='auto')
sv =  xr.open_zarr(dr['sv'], chunks='auto')

# Plotting
fig, axes = plt.subplots(5, 1, figsize=(30, 8), sharex=True, sharey=True)

sv.sv.sel(frequency = 38000).plot(ax=axes[0], y="range", x="ping_time", norm=LogNorm())
plt.gca().invert_yaxis()

atc_work.annotation.sel(category = 12).plot(ax=axes[1], y="range", x="ping_time")
plt.gca().invert_yaxis()

#atc.annotation.sel(category = 1000005).plot(ax=axes[2], y="range", x="ping_time")
_atc_rolf = atc_rolf.sum(dim="category")
_atc_rolf.annotation.plot(ax=axes[2], y="range", x="ping_time")
plt.gca().invert_yaxis()

_atc_korona = atc_korona.sum(dim="category")
_atc_korona.annotation.plot(ax=axes[3], y="range", x="ping_time")
plt.gca().invert_yaxis()

_atc_docker = atc_docker.sum(dim="category")
_atc_docker.annotation.plot(ax=axes[4], y="range", x="ping_time")
plt.gca().invert_yaxis()

plt.show()
