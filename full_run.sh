#

# S2000012_PGOSARS_1024
#export CRUISE=S2000012_PGOSARS_1024
#uv run macvin-ek500conversion --cruise "$CRUISE"
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
#uv run macvin-preprocess2zarr --cruise "$CRUISE"
#uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"

# S2001013_PGOSARS_1024
#export CRUISE=S2001013_PGOSARS_1024
#uv run macvin-ek500conversion --cruise "$CRUISE"
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
#uv run macvin-preprocess2zarr --cruise "$CRUISE"
##uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"

# S2002015_PGOSARS_1024
#export CRUISE=S2002015_PGOSARS_1024
#uv run macvin-ek500conversion --cruise "$CRUISE"
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
#uv run macvin-preprocess2zarr --cruise "$CRUISE"
#uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"

# S2003112_PGOSARS_4174
#export CRUISE=S2003112_PGOSARS_4174
#uv run macvin-ek500conversion --cruise "$CRUISE"
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
##uv run macvin-preprocess2zarr --cruise "$CRUISE"
##uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"

# S2004113_PGOSARS_4174
#export CRUISE=S2004113_PGOSARS_4174
#uv run macvin-ek500conversion --cruise "$CRUISE"
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
##uv run macvin-preprocess2zarr --cruise "$CRUISE"
##uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"

# S2005114_PGOSARS_4174  'ping_time' not strictly increasing in /PREPROCESSING/tokt2005114-D20051109-T021146.nc
export CRUISE=S2005114_PGOSARS_4174
uv run macvin-idxprocessing --cruise "$CRUISE"
uv run macvin-preprocessing --cruise "$CRUISE"
uv run macvin-atcprocessing --cruise "$CRUISE"
##uv run macvin-preprocess2zarr --cruise "$CRUISE"
##uv run macvin-atc2zarr --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

# S2006212_PJOHANHJORT_1019
#export CRUISE=S2006212_PJOHANHJORT_1019
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
##uv run macvin-preprocess2zarr --cruise "$CRUISE"
##uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"

# 'ping_time' not strictly increasing in /PREPROCESSING/tokt2005114-D20051109-T021146.nc
export CRUISE=S2007211_PJOHANHJORT_1019
uv run macvin-idxprocessing --cruise "$CRUISE"
uv run macvin-preprocessing --cruise "$CRUISE"
uv run macvin-atcprocessing --cruise "$CRUISE"
##uv run macvin-preprocess2zarr --cruise "$CRUISE"
##uv run macvin-atc2zarr --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

# 'ping_time' not strictly increasing in /PREPROCESSING/2012843-D20121002-T202452.nc
export CRUISE=S2012842_PCHRISTINAE_2704
uv run macvin-idxprocessing --cruise "$CRUISE"
uv run macvin-preprocessing --cruise "$CRUISE"
uv run macvin-atcprocessing --cruise "$CRUISE"
#uv run macvin-preprocess2zarr --cruise "$CRUISE"
#uv run macvin-atc2zarr --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"


# S2012843_PBRENNHOLM_4405
export CRUISE=S2012843_PBRENNHOLM_4405 # ValueError: conflicting sizes for dimension 'ping_time': length 1333071 on 'ping_time' and length 1332990 on {'category': 'annotation', 'ping_time': 'annotation', 'range': 'annotation'}
uv run macvin-idxprocessing --cruise "$CRUISE"
uv run macvin-preprocessing --cruise "$CRUISE"
uv run macvin-atcprocessing --cruise "$CRUISE"
#uv run macvin-preprocess2zarr --cruise "$CRUISE"
#uv run macvin-atc2zarr --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

# S1504S_PSCOTIA_MXHR6
#export CRUISE=S1504S_PSCOTIA_MXHR6
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
##uv run macvin-preprocess2zarr --cruise "$CRUISE"
##uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"

# S1412S_PSCOTIA_MXHR6
#export CRUISE=S1412S_PSCOTIA_MXHR6
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
#uv run macvin-preprocess2zarr --cruise "$CRUISE"
#uv run macvin-atc2zarr --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
#uv run macvin-lufreports --cruise "$CRUISE"
#uv run macvin-status --cruise "$CRUISE"
