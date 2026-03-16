#!/usr/bin/env python3

# Rename, clean, and geographically subset CESM2-LE NetCDF files
# Keeps only: output variable, lat, lon, time
# New filename format:
# cesm2-le.{output_variable}.{ensemble_member_number}.{time_period}.nc

# ============================================================
# 1) import statements
# ============================================================

import re
from pathlib import Path
from collections import defaultdict

import xarray as xr
from livio_intern_project import config, misc


# ============================================================
# 2) user input parameters
# ============================================================

# Variable in the raw input files
variable = "PRECT"

# Variable name to use in processed output files
output_variable = "tp24"

# Directory containing the original NetCDF files
input_dir = Path(config.dirs["raw_cesm2_le"] + output_variable + "/")

# Directory for renamed/cleaned files
output_dir = input_dir

# File search pattern
file_pattern = "*.nc"

# If True:
#   - write cleaned file to a temporary file
#   - replace the original file
#   - final filename becomes the new simplified filename
# If False:
#   - keep original files untouched
#   - write cleaned renamed files to output_dir
replace_original_files = True

# If True, overwrite existing output files
overwrite = False

# Require exactly this many files per time period
expected_files_per_period = 100

# ------------------------------------------------------------
# Select which time-period groups to process
# ------------------------------------------------------------
selected_time_periods = [
    "19200101-19291231",
    "19300101-19391231",
    "19400101-19491231",
    "19500101-19591231",
]

# ------------------------------------------------------------
# Maximum number of files to process within each selected group
# ------------------------------------------------------------
max_files_per_group = 100

# ------------------------------------------------------------
# Geographical bounds for subsetting
# ------------------------------------------------------------
lon_min = 2.0
lon_max = 32.5
lat_min = 53.0
lat_max = 73.5


# ============================================================
# 3) functions
# ============================================================

def extract_time_period(filename):
    """
    Extract time period string from filename, e.g. 20000101-20091231.
    Returns None if not found.
    """
    match = re.search(r"(\d{8}-\d{8})\.nc$", filename)
    if match:
        return match.group(1)
    return None


def find_input_files(input_dir, file_pattern="*.nc"):
    """
    Return sorted list of NetCDF files in input_dir.
    """
    return sorted(Path(input_dir).glob(file_pattern))


def group_files_by_time_period(files):
    """
    Group files by time period found in filename.
    Returns dictionary: {time_period: [file1, file2, ...]}
    """
    groups = defaultdict(list)

    for path in files:
        time_period = extract_time_period(path.name)
        if time_period is None:
            print(f"Skipping file with unrecognized time period: {path.name}")
            continue
        groups[time_period].append(path)

    for time_period in groups:
        groups[time_period] = sorted(groups[time_period])

    return dict(groups)


def select_groups(groups, selected_time_periods=None):
    """
    Select only requested time periods.
    If selected_time_periods is None, return all groups.
    """
    if selected_time_periods is None:
        return groups

    selected = {}
    for period in selected_time_periods:
        if period not in groups:
            print(f"Warning: requested time period not found: {period}")
            continue
        selected[period] = groups[period]

    return selected


def limit_files_per_group(groups, max_files_per_group=None):
    """
    Keep only the first max_files_per_group files in each group.
    If max_files_per_group is None, keep all files.
    """
    if max_files_per_group is None:
        return groups

    limited = {}
    for period, files in groups.items():
        limited[period] = files[:max_files_per_group]

    return limited


def make_output_filename(output_variable, ensemble_number, time_period):
    """
    Build output filename:
    cesm2-le.{output_variable}.{ensemble_member_number}.{time_period}.nc
    """
    return f"cesm2-le.{output_variable}.{ensemble_number:03d}.{time_period}.nc"


def keep_only_required_variables(ds, variable):
    """
    Keep only the requested raw variable and coordinates lat, lon, time.
    """
    keep_vars = [variable, "lat", "lon", "time"]
    existing_vars = [name for name in keep_vars if name in ds.variables or name in ds.coords]

    missing = [name for name in keep_vars if name not in ds.variables and name not in ds.coords]
    if missing:
        raise ValueError(f"Missing required variables/coords in dataset: {missing}")

    ds_out = ds[existing_vars].copy()

    coord_names = [name for name in ["lat", "lon", "time"] if name in ds_out]
    ds_out = ds_out.set_coords(coord_names)

    if "time" in ds_out and "bounds" in ds_out["time"].attrs:
        bounds_name = ds_out["time"].attrs["bounds"]
        if bounds_name not in ds_out.variables:
            del ds_out["time"].attrs["bounds"]

    return ds_out


def subset_geographical_area(ds, lon_min, lon_max, lat_min, lat_max):
    """
    Subset dataset to requested lat/lon region.

    Supports 1D lat/lon coordinates and 2D lat/lon coordinates.
    """
    if "lon" not in ds or "lat" not in ds:
        raise ValueError("Dataset must contain 'lat' and 'lon'.")

    lon = ds["lon"]
    lat = ds["lat"]

    # Case 1: 1D lat/lon
    if lon.ndim == 1 and lat.ndim == 1:
        lon_slice = slice(lon_min, lon_max) if lon.values[0] < lon.values[-1] else slice(lon_max, lon_min)
        lat_slice = slice(lat_min, lat_max) if lat.values[0] < lat.values[-1] else slice(lat_max, lat_min)
        ds_sub = ds.sel(lon=lon_slice, lat=lat_slice)

    # Case 2: 2D lat/lon
    elif lon.ndim == 2 and lat.ndim == 2:
        mask = (
            (lon >= lon_min) & (lon <= lon_max) &
            (lat >= lat_min) & (lat <= lat_max)
        )
        ds_sub = ds.where(mask, drop=True)

    else:
        raise ValueError("Unsupported lat/lon coordinate structure.")

    return ds_sub


def process_one_file(
    infile,
    outfile,
    variable,
    output_variable,
    lon_min,
    lon_max,
    lat_min,
    lat_max,
    overwrite=False,
):
    """
    Open one NetCDF file, keep only variable/lat/lon/time,
    subset to requested lat/lon region, rename the output variable,
    and write to outfile.
    """
    infile = Path(infile)
    outfile = Path(outfile)

    outfile.parent.mkdir(parents=True, exist_ok=True)

    if outfile.exists() and not overwrite:
        print(f"Output exists, skipping: {outfile}")
        return outfile

    if outfile.exists() and overwrite:
        outfile.unlink()

    print(f"Reading : {infile}")
    print(f"Writing : {outfile}")
    print(f"Subset bounds: lon {lon_min} to {lon_max}, lat {lat_min} to {lat_max}")

    with xr.open_dataset(infile, decode_times=False) as ds:
        ds_out = keep_only_required_variables(ds, variable)
        ds_out = subset_geographical_area(
            ds_out,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
        )

        # Convert PRECT from m/s to mm/day and rename to tp24
        if variable == "PRECT":
            tp24 = ds_out[variable] * 86400000.0
            tp24.attrs = ds_out[variable].attrs.copy()
            tp24.attrs["units"] = "mm/day"
            tp24.attrs["long_name"] = "Total daily precipitation"

            ds_out = xr.Dataset(
                data_vars={output_variable: tp24},
                coords={
                    "time": ds_out["time"],
                    "lat": ds_out["lat"],
                    "lon": ds_out["lon"],
                },
                attrs=ds_out.attrs,
            )
        else:
            ds_out = ds_out.rename({variable: output_variable})

        ds_out.to_netcdf(outfile)

    return outfile


def process_period_group(
    files,
    variable,
    output_variable,
    time_period,
    output_dir,
    lon_min,
    lon_max,
    lat_min,
    lat_max,
    overwrite=False,
    replace_original_files=False,
):
    """
    Process all files for one time period.
    Files are sorted and assigned ensemble numbers 001, 002, ..., N.
    """
    n_files = len(files)

    for i, infile in enumerate(files, start=1):
        new_name = make_output_filename(output_variable, i, time_period)

        if replace_original_files:
            temp_outfile = infile.parent / f"tmp_{new_name}"
            final_outfile = infile.parent / new_name

            process_one_file(
                infile=infile,
                outfile=temp_outfile,
                variable=variable,
                output_variable=output_variable,
                lon_min=lon_min,
                lon_max=lon_max,
                lat_min=lat_min,
                lat_max=lat_max,
                overwrite=overwrite,
            )

            if infile.resolve() != final_outfile.resolve():
                infile.unlink(missing_ok=True)

            if final_outfile.exists():
                if overwrite:
                    final_outfile.unlink()
                else:
                    raise FileExistsError(f"Final file already exists: {final_outfile}")

            temp_outfile.rename(final_outfile)

        else:
            outfile = Path(output_dir) / new_name
            process_one_file(
                infile=infile,
                outfile=outfile,
                variable=variable,
                output_variable=output_variable,
                lon_min=lon_min,
                lon_max=lon_max,
                lat_min=lat_min,
                lat_max=lat_max,
                overwrite=overwrite,
            )

    print(f"Finished time period {time_period}: {n_files} files")


def validate_groups(groups, expected_files_per_period=None):
    """
    Print summary and optionally warn if a group does not contain
    the expected number of files.
    """
    print("\nSummary of detected time periods:")
    for time_period in sorted(groups):
        n = len(groups[time_period])
        print(f"  {time_period}: {n} files")

        if expected_files_per_period is not None and n != expected_files_per_period:
            print(
                f"  WARNING: expected {expected_files_per_period} files for {time_period}, "
                f"but found {n}"
            )


def print_selected_groups(groups):
    """
    Print selected groups and filenames.
    """
    print("\nGroups that will be processed:")
    for period in sorted(groups):
        files = groups[period]
        print(f"  {period}: {len(files)} files")
        for f in files[:5]:
            print(f"     {f.name}")
        if len(files) > 5:
            print("     ...")


# ============================================================
# 4) main script
# ============================================================

if __name__ == "__main__":

    files = find_input_files(input_dir=input_dir, file_pattern=file_pattern)

    if len(files) == 0:
        raise FileNotFoundError(f"No files found in {input_dir} matching pattern {file_pattern}")

    print(f"Found {len(files)} NetCDF files in {input_dir}")

    groups = group_files_by_time_period(files)
    validate_groups(groups, expected_files_per_period=expected_files_per_period)

    groups = select_groups(groups, selected_time_periods=selected_time_periods)
    groups = limit_files_per_group(groups, max_files_per_group=max_files_per_group)

    if len(groups) == 0:
        raise ValueError("No groups left to process after applying selection.")

    print_selected_groups(groups)

    if not replace_original_files:
        output_dir.mkdir(parents=True, exist_ok=True)

    for time_period in sorted(groups):
        print()
        print(f"Processing time period: {time_period}")

        process_period_group(
            files=groups[time_period],
            variable=variable,
            output_variable=output_variable,
            time_period=time_period,
            output_dir=output_dir,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
            overwrite=overwrite,
            replace_original_files=replace_original_files,
        )

    print("\nDone.")
