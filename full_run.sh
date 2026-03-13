#uv run macvin-ek500conversion

# S2000012_PGOSARS_1024 sv_ok, atc_ok, running report
export CRUISE=S2000012_PGOSARS_1024
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
#uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"

# S2001013_PGOSARS_1024 updated_filelist
export CRUISE=S2000012_PGOSARS_1024
uv run macvin-idxprocessing --cruise "$CRUISE"
uv run macvin-preprocessing --cruise "$CRUISE"
uv run macvin-atcprocessing --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

# S2002015_PGOSARS_1024 sv_ok, atc_ok
export CRUISE=S2002015_PGOSARS_1024
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

# S2003112_PGOSARS_4174 sv_ok, atc_ok
export CRUISE=S2003112_PGOSARS_4174
#uv run macvin-idxprocessing --cruise "$CRUISE"
#uv run macvin-preprocessing --cruise "$CRUISE"
#uv run macvin-atcprocessing --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

# S2004113_PGOSARS_4174 not complete
export CRUISE=S2004113_PGOSARS_4174
uv run macvin-idxprocessing --cruise "$CRUISE"
uv run macvin-preprocessing --cruise "$CRUISE"
uv run macvin-atcprocessing --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

# S2006212_PJOHANHJORT_1019 not complete
export CRUISE=S2006212_PJOHANHJORT_1019
uv run macvin-idxprocessing --cruise "$CRUISE"
uv run macvin-preprocessing --cruise "$CRUISE"
uv run macvin-atcprocessing --cruise "$CRUISE"
uv run macvin-reports --cruise "$CRUISE"
uv run macvin-lufreports --cruise "$CRUISE"
uv run macvin-status --cruise "$CRUISE"

