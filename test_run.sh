uv run macvin-status --quick-run --cruise S2000012_PGOSARS_1024 
uv run macvin-idxprocessing --dry-run --cruise S2000012_PGOSARS_1024 
uv run macvin-preprocessing --dry-run --cruise S2000012_PGOSARS_1024 
uv run macvin-preprocess2zarr --dry-run --cruise S2000012_PGOSARS_1024 
uv run macvin-atcprocessing --dry-run --cruise S2000012_PGOSARS_1024
uv run macvin-atc2zarr --dry-run --cruise S2000012_PGOSARS_1024
uv run macvin-reports --dry-run --cruise S2000012_PGOSARS_1024
uv run macvin-lufreports --dry-run --cruise S2000012_PGOSARS_1024

#uv run macvin-status --quick-run
#uv run macvin-idxprocessing --dry-run
#uv run macvin-preprocessing --dry-run
#uv run macvin-reports --dry-run
#uv run macvin-lufreports --dry-run
#uv run macvin-test



