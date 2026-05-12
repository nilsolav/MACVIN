BASE=/data/s3/MACWIN-scratch/silver
REL=ACOUSTIC/EK/TARGET_CLASSIFICATION/korona_noisefiltering/mackerel_korneliussen2016

for cruise_dir in "$BASE"/*; do
    [ -d "$cruise_dir" ] || continue

    src="$cruise_dir/$REL"
    dst="$src/sv_nc"

    [ -d "$src" ] || continue

    mkdir -p "$dst"

    # Move only .nc files directly in src, not recursively
    find "$src" -maxdepth 1 -type f -name "*.nc" -exec mv -t "$dst" {} +

done
