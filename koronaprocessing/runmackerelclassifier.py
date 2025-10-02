#import KoronaScript as ks
#import KoronaScript.Modules as ksm
from pathlib import Path
import os
#import subprocess
import xarray as xr
import zarr
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

crimac_scratch = os.getenv('CRIMACSCRATCH')
survey = 'S2005114_PGOSARS_4174'

# For test data
dr = {}
dr['raw'] = Path('/data/cruise_data/2012/S2012843_PBRENNHOLM_4405/EK60Raw')
dr['korona_mackerel_v1'] = Path(crimac_scratch) / Path('test_data') / Path(
    survey) / Path('ACOUSTIC/TARGET_CLASSIFICATION/korona_mackerel_v1/')
dr['gridded'] = Path(crimac_scratch) / Path('test_data_out') / Path(survey) / Path('ACOUSTIC/GRIDDED')
dr['label'] =  str(dr['gridded'])+'/S2005114_PGOSARS_4174_labels.zarr'
dr['sv'] =  str(dr['gridded'])+'/S2005114_PGOSARS_4174_sv.zarr'


atc = xr.open_mfdataset(str(dr['korona_mackerel_v1'])+'/*.nc', combine='by_coords')
lbl = xr.open_zarr(dr['label'], chunks='auto')
sv =  xr.open_zarr(dr['sv'], chunks='auto')

# Plotting

fig, axes = plt.subplots(4, 1, figsize=(20, 8), sharex=True, sharey=True)

sv.sv.sel(frequency = 38000).plot(ax=axes[0], y="range", x="ping_time", norm=LogNorm())
plt.gca().invert_yaxis()

lbl.annotation.sel(category = 12).plot(ax=axes[1], y="range", x="ping_time")
plt.gca().invert_yaxis()

atc.annotation.sel(category = 1000004).plot(ax=axes[2], y="range", x="ping_time")
plt.gca().invert_yaxis()

atc.annotation.sel(category = 1000005).plot(ax=axes[3], y="range", x="ping_time")
plt.gca().invert_yaxis()


plt.show()


'''
ks = ks.KoronaScript(Categorization='categorization.xml',
                     HorizontalTransducerOffsets='HorizontalTransducerOffsets.xml')

ks.add(ksm.EmptyPingRemoval())
ks.add(ksm.Categorization())
ks.write()
ks.run(src=input_dir, dst=output_dir)

'''
