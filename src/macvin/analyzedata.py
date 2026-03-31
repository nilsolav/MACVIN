from pathlib import Path
import logging
import xarray as xr
import matplotlib.pyplot as plt
import numpy as np
from macvin.flows import get_survey
import dask.array as da
import os

logger = logging.getLogger(__name__)


def macvin_consistency_flow(
    dry_run: bool = False,
    cruise: str | None = None,
    quick_run: bool = True
):
    
    logger.info("#### Running consistency plot ####")

    if quick_run:
        logger.info('Test run')
        crimacsratch = Path(os.getenv("CRIMACSCRATCH"))
        dat = crimacsratch / Path("test_data_azure/",
                                  "S2005114_PGOSARS_4174/ACOUSTIC/EK/")
        results = crimacsratch / Path("test_data_azure_silver/",
                                      "S2005114_PGOSARS_4174/ACOUSTIC/EK/")

        sv_pre_f = dat / Path("PREPROCESSING/korona_preprocessing/sv_nc/")
        sv_noise_f = dat / Path("PREPROCESSING/korona_noisefiltering/sv_nc/")
        labels_f = dat / Path("TARGET_CLASSIFICATION/korona_noisefiltering/"
                              "mackerel_korneliussen2016/labels_nc/")
        dataqc_f = results / Path("QUALITY_CONTROL/macvin")
        dataqc_f.mkdir(parents=True, exist_ok=True)

        logger.info(sv_pre_f)
        logger.info(sv_noise_f)
        logger.info(labels_f)
        logger.info(dataqc_f)

    else:
        df, exclude_files = get_survey(cruise=cruise)
        logger.info('test2')

    calculate_dist(sv_pre_f, sv_noise_f, labels_f, dataqc_f, quick_run)


def calculate_dist(sv_pre_f, sv_noise_f, labels_f, dataqc_f, quick_run=True):
    
    # Calculate the sv mackerel distribution
    chunks = {"frequency": 1, "ping_time": 2048}

    sv_pre = (xr.open_mfdataset(str(sv_pre_f)+"/*.nc",
                                chunks=chunks,
                                combine="by_coords")
                .sortby("frequency")
              )
    sv_pre = depthtorange(sv_pre)

    sv_noise = (xr.open_mfdataset(str(sv_noise_f)+"/*.nc",
                                  chunks = chunks,
                                  combine="by_coords")
                .sortby("frequency")
                )
    sv_noise = depthtorange(sv_noise)

    labels = xr.open_mfdataset(str(labels_f)+"/*.nc",
                               chunks=chunks,
                               combine="by_coords")

    # Create bottom mask
    bottom_noise =  bottom_mask_single_freq(sv_noise, sv_noise)

    # Create filtered histogram
    res1 = compute_sv_histogram_dask(
        ds_sv=sv_noise,
        ds_annotation=labels,
        ds_bottom=bottom_noise,
        frequency=38000,
        category=1000004,
        bins=100,
    )

    # Histogram without filtering on bottom
    res2 = compute_sv_histogram_dask(
        ds_sv=sv_pre,
        ds_annotation=labels,
        ds_bottom=None,
        frequency=38000,
        category=1000004,
        bins=100,
    )

    # Store only histogram

    # Plot figures
    fig1, ax = plot_sv_with_bottoms(
        sv_ds=sv_noise,
        bottom_ds_1=sv_pre,
        bottom_ds_2=sv_noise,
        frequency=38000,
        label_1="bottom from sv_pre",
        label_2="bottom from sv_noise",
        cmap="inferno",
        robust=True,
    )

    fig1.savefig("sv.png", dpi=300, bbox_inches="tight")
    
    sv_noise["sv"] = 10**(res1["sv_masked"].expand_dims(
        frequency=[38000.0])/10)
    
    fig2, ax = plot_sv_with_bottoms(
        sv_ds=sv_noise,
        bottom_ds_1=sv_pre,
        bottom_ds_2=sv_noise,
        frequency=38000,
        label_1="bottom from sv_pre",
        label_2="bottom from sv_noise",
        cmap="inferno",
        robust=True,
    )

    fig2.savefig(str(dataqc_f / Path("sv_mackerel.png")),
                 dpi=300, bbox_inches="tight")

    fig3, ax = plot_sv_histogram_comparison(
        res1["hist"],
        res1["bin_edges"],
        res2["hist"],
        res2["bin_edges"],
        label1="Mackerel Sv without bottom",
        label2="Mackerel Sv",
        title="Sv histogram comparison (38 kHz)",
    )

    fig3.savefig(str(dataqc_f / Path("sv_histogram.png")),
                 dpi=150)

    if quick_run:  # Only plot figures in quick run mode
        plt.show()
    else:
        plt.close(fig1)
        plt.close(fig2)
        plt.close(fig3)


def plot_sv_histogram_comparison(
    hist1,
    bin_edges1,
    hist2,
    bin_edges2,
    label1="Dataset 1",
    label2="Dataset 2",
    title: str | None = None,
    xlabel: str = "sv",
    ylabel: str = "Count",
    style: str = "step",  # "step" or "bar"
):
    """
    Plot two histograms in the same axes for comparison.

    Parameters
    ----------
    hist1, hist2 : array-like
    bin_edges1, bin_edges2 : array-like
    style : "step" (recommended) or "bar"
    """

    fig, ax = plt.subplots(figsize=(8, 5))

    if style == "bar":
        # Bars (slightly shifted + transparent)
        width1 = np.diff(bin_edges1)
        width2 = np.diff(bin_edges2)

        ax.bar(
            bin_edges1[:-1],
            hist1,
            width=width1,
            align="edge",
            alpha=0.5,
            label=label1,
        )

        ax.bar(
            bin_edges2[:-1],
            hist2,
            width=width2,
            align="edge",
            alpha=0.5,
            label=label2,
        )

    elif style == "step":
        # Step plots (better for comparison)
        centers1 = 0.5 * (bin_edges1[:-1] + bin_edges1[1:])
        centers2 = 0.5 * (bin_edges2[:-1] + bin_edges2[1:])

        ax.step(centers1, hist1, where="mid", label=label1)
        ax.step(centers2, hist2, where="mid", label=label2)

    else:
        raise ValueError("style must be 'step' or 'bar'")

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    if title:
        ax.set_title(title)

    ax.legend()
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    return fig, ax


def plot_sv_histogram(
    hist,
    bin_edges,
    title: str | None = None,
    xlabel: str = "sv",
    ylabel: str = "Count",
):
    """
    Plot histogram and return matplotlib figure + axis.
    """

    fig, ax = plt.subplots(figsize=(8, 5))

    widths = np.diff(bin_edges)
    ax.bar(bin_edges[:-1], hist, width=widths, align="edge")

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    if title:
        ax.set_title(title)

    fig.tight_layout()

    return fig, ax


def compute_sv_histogram_dask(
    ds_sv: xr.Dataset,
    ds_annotation: xr.Dataset = None,
    ds_bottom: xr.Dataset = None,
    frequency: float = 38000.0,
    category: int = 1000004,
    annotation_threshold: float = 0.0,
    bins: int = 100,
    sv_min: float = -90.0,
    sv_max: float = 30.0,
    method: str = "nearest",
):
    """
    Compute histogram of sv masked by bottom mask and annotation category,
    without materializing all sv values in memory.

    Returns
    -------
    dict with:
        - hist         : numpy.ndarray
        - bin_edges    : numpy.ndarray
        - sv_masked    : xr.DataArray (lazy)
        - combined_mask: xr.DataArray (lazy)
    """

    # Convert to Sv
    ds_sv = ds_sv.copy()
    ds_sv["sv"] = 10 * np.log10(ds_sv["sv"])

    # Select one frequency
    sv = ds_sv["sv"].sel(frequency=frequency, method=method)

    # Bottom mask
    if ds_bottom is not None:
        bottom_mask = ds_bottom["bottom_range"]
    else:
        bottom_mask = xr.ones_like(sv, dtype=bool)

    # Annotation mask
    if ds_annotation is not None:
        ann = ds_annotation["annotation"]
        if "category" in ann.dims:
            ann_mask = ann.sel(category=category) > annotation_threshold
        else:
            ann_mask = ann == category
    else:
        ann_mask = xr.ones_like(sv, dtype=bool)

    # Align dimensions/coordinates
    sv, bottom_mask, ann_mask = xr.align(sv, bottom_mask, ann_mask, join="exact")

    combined_mask = bottom_mask & ann_mask

    # Keep a lazy xarray version around for later use if wanted
    sv_masked = sv.where(combined_mask)

    # Dask arrays
    sv_da = sv.data
    mask_da = combined_mask.data

    # Exclude NaN/inf too
    valid_mask = mask_da & da.isfinite(sv_da)

    bin_edges = np.linspace(sv_min, sv_max, bins + 1)

    # Put invalid points outside the histogram range and give them weight 0
    safe_sv = da.where(valid_mask, sv_da, sv_min - 1.0)
    weights = valid_mask.astype(np.int64)

    hist_da, bin_edges_da = da.histogram(
        safe_sv.ravel(),
        bins=bin_edges,
        weights=weights.ravel(),
        density=False,
    )

    # Only compute the small outputs
    hist, bin_edges = da.compute(hist_da, bin_edges_da)

    return {
        "hist": hist,
        "bin_edges": bin_edges,
        "sv_masked": sv_masked,
        "combined_mask": combined_mask,
    }


def bottom_mask_single_freq(
    ds_sv: xr.Dataset,
    ds_bottom: xr.Dataset,
    frequency: float = 38000.0,
) -> xr.Dataset:
    """
    Create a boolean mask for a single frequency:
    True where range < bottom_depth.

    Parameters
    ----------
    ds_sv : xr.Dataset
        Dataset containing `sv` with dims (frequency, ping_time, range)
    ds_bottom : xr.Dataset
        Dataset containing `bottom_depth`
    frequency : float
        Frequency to select (default: 38000 Hz)

    Returns
    -------
    xr.Dataset
        Dataset containing boolean mask `bottom_noise` with dims (ping_time, range)
    """

    # Select frequency
    sv = ds_sv["sv"].sel(frequency=frequency)
    bottom = ds_bottom["bottom_depth"]

    # Align ping_time (important!)
    sv, bottom = xr.align(sv, bottom, join="exact")

    # If bottom has frequency dimension, select same frequency
    if "frequency" in bottom.dims:
        bottom = bottom.sel(frequency=frequency)

    # Broadcast happens automatically
    mask = sv["range"] < bottom

    bottom_range_ds = (
        mask
        .transpose("ping_time", "range")
        .to_dataset(name="bottom_range")
    )
    return bottom_range_ds


def depthtorange(sv):
    sv["bottom_depth"] = (sv["bottom_depth"] -
                          sv["transducer_draft"] -
                          sv["heave"]
                          )
    return sv


def plot_sv_with_bottoms(
    sv_ds,
    bottom_ds_1,
    bottom_ds_2,
    frequency=38000,
    sv_var="sv",
    bottom_var="bottom_depth",
    ax=None,
    figsize=(14, 6),
    cmap="inferno",
    vmin=-80,
    vmax=20,
    label_1="bottom 1",
    label_2="bottom 2",
    color_1 = "green",
    color_2 = "blue",
    linewidth=1,
    robust=False,
):
    sv_ds = sv_ds.sortby("frequency")

    sv = sv_ds[sv_var].sel(frequency=frequency, method="nearest")

    # Transform to Sv
    sv = 10 * np.log10(sv.clip(min=1e-10))

    selected_freq = float(sv["frequency"].values)

    b1 = bottom_ds_1[bottom_var].sel(frequency=selected_freq, method="nearest")
    b2 = bottom_ds_2[bottom_var].sel(frequency=selected_freq, method="nearest")

    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig = ax.figure

    # Put data in plotting order and use larger chunks before compute
    sv2d = sv.transpose("range", "ping_time")

    if hasattr(sv2d.data, "rechunk"):
        sv2d = sv2d.chunk({"range": -1, "ping_time": 500})

    # Explicit compute once, instead of letting matplotlib trigger it awkwardly
    sv2d = sv2d.compute()
    b1 = b1.compute()
    b2 = b2.compute()

    if robust and (vmin is None or vmax is None):
        vals = sv2d.values
        if vmin is None:
            vmin = np.nanpercentile(vals, 2)
        if vmax is None:
            vmax = np.nanpercentile(vals, 98)

    pcm = ax.pcolormesh(
        sv2d["ping_time"].values,
        sv2d["range"].values,
        sv2d.values,
        shading="auto",
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
    )

    ax.plot(b1["ping_time"].values, b1.values, label=label_1,
            linewidth=linewidth, color=color_1)
    ax.plot(b2["ping_time"].values, b2.values, label=label_2,
            linewidth=linewidth, color=color_2)

    ax.set_xlabel("Ping time")
    ax.set_ylabel("Range / depth [m]")
    ax.set_title(f"{sv_var} at {selected_freq/1000:.0f} kHz")
    ax.invert_yaxis()
    ax.legend(loc="upper left")
    fig.colorbar(pcm, ax=ax, label=sv_var)
    fig.autofmt_xdate()

    return fig, ax
