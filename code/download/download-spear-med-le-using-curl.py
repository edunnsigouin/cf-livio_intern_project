#!/usr/bin/env python3
"""
Download and post-process SPEAR-MED precipitation files.

For each selected simulation, ensemble member, and time period, the script:
1) downloads the original NetCDF file with curl
2) converts pr from kg m-2 s-1 to mm/day
3) subsets the data to user-defined lat/lon bounds
4) keeps only time, lat, lon, and the requested output variable
5) saves a renamed processed NetCDF file
"""

# ==========================================================
# 1. Import statements
# ==========================================================

import os
import re
import subprocess
import xarray as xr
from livio_intern_project import config, misc

# ==========================================================
# 2. User-defined input parameters
# ==========================================================

# Base FTP location of dataset
FTP_SITE = "ftp://nomads.gfdl.noaa.gov/2/GFDL-LARGE-ENSEMBLES/CMIP/NOAA-GFDL/GFDL-SPEAR-MED"

# Variable metadata used in the remote archive
VARIABLE = "pr"              # variable name on remote server / in raw files
OUTPUT_VARIABLE = "tp24"     # variable name in processed output files
FREQ = "day"
GRID = "gr3"
VERSION = "v20210201"

# Name used on the remote server when building download paths and filenames
MODEL_REMOTE = "GFDL-SPEAR-MED"

# Name used in the final processed NetCDF filenames
MODEL_LOCAL = "gfdl_spear_med_le"

# Output directory for downloaded and processed files
OUTDIR = config.dirs["raw_gfdl_spear_med_le"] + OUTPUT_VARIABLE + "/"

# Geographic subset to keep (scandinavia)
LON_MIN = 2.0
LON_MAX = 32.5
LAT_MIN = 53.0
LAT_MAX = 73.5

# Maximum number of source files handled in one run
MAX_FILES = 30

# Skip work if the final processed file already exists
SKIP_EXISTING = True

# Remove raw downloaded file after processing
DELETE_RAW_FILE = True

# Simulation types to download
# SIMULATIONS = ["historical", "scenarioSSP5-85"]
SIMULATIONS = ["historical"]

# Ensemble members to download
MEMBERS = [
     "r1i1p1f1","r2i1p1f1","r3i1p1f1","r4i1p1f1","r5i1p1f1",
     "r6i1p1f1","r7i1p1f1","r8i1p1f1","r9i1p1f1","r10i1p1f1",
     "r11i1p1f1","r12i1p1f1","r13i1p1f1","r14i1p1f1","r15i1p1f1",
     "r16i1p1f1","r17i1p1f1","r18i1p1f1","r19i1p1f1","r20i1p1f1",
     "r21i1p1f1","r22i1p1f1","r23i1p1f1","r24i1p1f1","r25i1p1f1",
     "r26i1p1f1","r27i1p1f1","r28i1p1f1","r29i1p1f1","r30i1p1f1"
]
#MEMBERS = ["r1i1p1f1", "r2i1p1f1", "r3i1p1f1"]

# Historical simulation periods
# HISTORICAL_PERIODS = [
#     "19210101-19301231",
#     "19310101-19401231",
#     "19410101-19501231",
#     "19510101-19601231",
#     "19610101-19701231",
#     "19710101-19801231",
#     "19810101-19901231",
#     "19910101-20001231",
#     "20010101-20101231",
#     "20110101-20141231",
# ]
HISTORICAL_PERIODS = ["19210101-19301231"]

# Scenario simulation periods
SCENARIO_PERIODS = [
    "20150101-20201231",
    "20210101-20301231",
    "20310101-20401231",
]


# ==========================================================
# 3. Functions
# ==========================================================

def get_periods(simulation):
    """Return the list of time periods corresponding to a simulation type."""
    if simulation == "historical":
        return HISTORICAL_PERIODS
    if simulation == "scenarioSSP5-85":
        return SCENARIO_PERIODS
    raise ValueError(f"Unknown simulation: {simulation}")


def build_remote_filename(simulation, member, period):
    """Build the filename used by the remote archive."""
    return f"{VARIABLE}_{FREQ}_{MODEL_REMOTE}_{simulation}_{member}_{GRID}_{period}.nc"


def build_url(simulation, member, remote_filename):
    """Build the full FTP URL for one source file."""
    return f"{FTP_SITE}/{simulation}/{member}/{FREQ}/{VARIABLE}/{GRID}/{VERSION}/{remote_filename}"


def download_file(url):
    """Download one file with curl and raise an error if the command fails."""
    cmd = [
        "curl",
        "-f",          # fail on server-side errors
        "-C", "-",     # resume partial downloads if possible
        "-O",          # save using the original remote filename
        "--retry", "3",
        url,
    ]
    subprocess.run(cmd, check=True)


def member_to_two_digit(member):
    """
    Convert a CMIP-style ensemble member string to a two-digit ID.

    Examples:
        r1i1p1f1  -> 01
        r10i1p1f1 -> 10
    """
    match = re.match(r"r(\d+)i\d+p\d+f\d+", member)
    if match is None:
        raise ValueError(f"Could not parse ensemble member: {member}")
    return f"{int(match.group(1)):02d}"


def build_processed_filename(variable, member, period):
    """
    Build the final processed filename.

    Example:
        gfdl_spear_med_le.tp24.01.19210101-19301231.nc
    """
    member_id = member_to_two_digit(member)
    return f"{MODEL_LOCAL}.{variable}.{member_id}.{period}.nc"


def convert_lon_to_dataset_convention(lon_value, lon_coord):
    """
    Convert input longitude to the dataset convention.

    If the dataset uses 0..360 and the user gives a negative longitude,
    shift it to the 0..360 range.
    """
    lon_min_ds = float(lon_coord.min())
    lon_max_ds = float(lon_coord.max())
    uses_0360 = lon_min_ds >= 0.0 and lon_max_ds > 180.0

    if uses_0360 and lon_value < 0.0:
        return lon_value % 360.0

    return lon_value


def subset_dataset(ds, lon_min, lon_max, lat_min, lat_max):
    """
    Subset the dataset to the requested lat/lon box.

    Handles both increasing and decreasing latitude coordinates, and also
    longitude ranges that cross the 0/360 seam.
    """
    lon_min = convert_lon_to_dataset_convention(lon_min, ds["lon"])
    lon_max = convert_lon_to_dataset_convention(lon_max, ds["lon"])

    # Latitude slice must follow the ordering in the file
    lat_values = ds["lat"].values
    if lat_values[0] < lat_values[-1]:
        ds = ds.sel(lat=slice(lat_min, lat_max))
    else:
        ds = ds.sel(lat=slice(lat_max, lat_min))

    # Standard longitude slice or wrap-around slice
    if lon_min <= lon_max:
        ds = ds.sel(lon=slice(lon_min, lon_max))
    else:
        ds_left = ds.sel(lon=slice(lon_min, float(ds["lon"].max())))
        ds_right = ds.sel(lon=slice(float(ds["lon"].min()), lon_max))
        ds = xr.concat([ds_left, ds_right], dim="lon")

    return ds


def process_file(raw_filename, processed_filename):
    """
    Process one downloaded NetCDF file.

    Steps:
    - subset to requested lat/lon bounds
    - convert pr from kg m-2 s-1 to mm/day
    - rename pr to tp24
    - keep only time, lat, lon, and tp24
    - write a compact processed NetCDF file
    """
    with xr.open_dataset(raw_filename, decode_times=False) as ds:

        # Subset the requested geographic region
        ds = subset_dataset(ds, LON_MIN, LON_MAX, LAT_MIN, LAT_MAX)

        # Convert precipitation flux to mm/day and rename to output variable
        tp24 = ds[VARIABLE] * 86400.0
        tp24.attrs = ds[VARIABLE].attrs.copy()
        tp24.attrs["units"] = "mm/day"

        # Keep a clear long_name if wanted
        if "long_name" in tp24.attrs:
            tp24.attrs["long_name"] = tp24.attrs["long_name"].replace(VARIABLE, OUTPUT_VARIABLE)
        else:
            tp24.attrs["long_name"] = "total precipitation in 24 hours"

        # Keep only the core coordinates and requested output variable
        ds_out = xr.Dataset(
            data_vars={OUTPUT_VARIABLE: tp24},
            coords={
                "time": ds["time"],
                "lat": ds["lat"],
                "lon": ds["lon"],
            },
            attrs=ds.attrs,
        )

        # Add a simple processing note to the history attribute
        old_history = ds_out.attrs.get("history", "")
        new_history = (
            f"Converted {VARIABLE} from kg m-2 s-1 to mm/day; "
            f"renamed {VARIABLE} to {OUTPUT_VARIABLE}; "
            f"subset to lon=[{LON_MIN}, {LON_MAX}], lat=[{LAT_MIN}, {LAT_MAX}]; "
            f"kept only time, lat, lon, {OUTPUT_VARIABLE}."
        )
        ds_out.attrs["history"] = f"{old_history}\n{new_history}".strip()

        # Preserve fill value if present and use light compression
        encoding = {OUTPUT_VARIABLE: {"zlib": True, "complevel": 4}}
        if "_FillValue" in ds[VARIABLE].attrs:
            encoding[OUTPUT_VARIABLE]["_FillValue"] = ds[VARIABLE].attrs["_FillValue"]

        ds_out.to_netcdf(processed_filename, format="NETCDF4", encoding=encoding)


def remove_file_if_requested(filename):
    """Delete a file if configured to do so."""
    if DELETE_RAW_FILE and os.path.exists(filename):
        os.remove(filename)


def handle_one_file(simulation, member, period):
    """
    Download and process one file.

    Returns True if a new processed file was created, otherwise False.
    """
    raw_filename = build_remote_filename(simulation, member, period)
    processed_filename = build_processed_filename(OUTPUT_VARIABLE, member, period)
    url = build_url(simulation, member, raw_filename)

    if SKIP_EXISTING and os.path.exists(processed_filename):
        print(f"Skipping existing processed file: {processed_filename}")
        return False

    # Download raw source file if not already present locally
    if not os.path.exists(raw_filename):
        print(f"Downloading raw file: {raw_filename}")
        print(f"URL: {url}")
        download_file(url)
    else:
        print(f"Raw file already exists locally: {raw_filename}")

    print(f"Processing file -> {processed_filename}")
    process_file(raw_filename, processed_filename)
    remove_file_if_requested(raw_filename)

    return True


# ==========================================================
# 4. Main script calling functions
# ==========================================================

def main():
    """Run the full download and post-processing workflow."""
    os.makedirs(OUTDIR, exist_ok=True)
    os.chdir(OUTDIR)

    processed_count = 0

    for simulation in SIMULATIONS:
        periods = get_periods(simulation)

        for member in MEMBERS:
            for period in periods:

                if processed_count >= MAX_FILES:
                    print(f"\nReached MAX_FILES={MAX_FILES}. Stopping.")
                    print(f"Created {processed_count} processed files.")
                    return

                try:
                    created = handle_one_file(simulation, member, period)
                    if created:
                        processed_count += 1

                except subprocess.CalledProcessError:
                    print(f"Download failed for {simulation}, {member}, {period}")

                except Exception as exc:
                    print(f"Processing failed for {simulation}, {member}, {period}")
                    print(f"Reason: {exc}")

    print(f"\nFinished. Created {processed_count} processed files.")


if __name__ == "__main__":
    main()
