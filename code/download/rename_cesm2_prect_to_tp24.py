#!/usr/bin/env python3

"""
Create new CESM2-LE NetCDF files where:

filename:
    cesm2-le.PRECT.001.19200101-19291231.nc
becomes
    cesm2-le.tp24.001.19200101-19291231.nc

variable inside file:
    PRECT
becomes
    tp24

Original files remain unchanged.
"""

# ============================================================
# 1) imports
# ============================================================

import re
from pathlib import Path
import xarray as xr
from livio_intern_project import config


# ============================================================
# 2) user parameters
# ============================================================

input_dir = Path(config.dirs["raw_cesm2_le"] + "tp24/")

input_variable = "PRECT"
output_variable = "tp24"

file_pattern = "cesm2-le.PRECT.*.nc"

# None → process ALL files
# integer → process only that many
max_files = None

overwrite = True


# ============================================================
# 3) functions
# ============================================================

def find_input_files(input_dir, file_pattern):
    return sorted(input_dir.glob(file_pattern))


def build_output_filename(infile_name):

    pattern = r"^cesm2-le\.PRECT\.(\d{3})\.(\d{8}-\d{8})\.nc$"
    match = re.match(pattern, infile_name)

    if match is None:
        raise ValueError(f"Filename does not match expected pattern: {infile_name}")

    ensemble_member = match.group(1)
    time_period = match.group(2)

    return f"cesm2-le.{output_variable}.{ensemble_member}.{time_period}.nc"


def rename_variable_in_dataset(ds):

    if input_variable not in ds.data_vars:
        raise ValueError(f"Variable '{input_variable}' not found.")

    ds_out = ds.rename({input_variable: output_variable})

    ds_out[output_variable].attrs = ds[input_variable].attrs.copy()
    ds_out[output_variable].attrs["long_name"] = "Total daily precipitation"
    ds_out[output_variable].attrs["units"] = "mm/day"

    history_old = ds_out.attrs.get("history", "")
    history_new = f"Copied file and renamed {input_variable} → {output_variable}"
    ds_out.attrs["history"] = f"{history_old}\n{history_new}".strip()

    return ds_out


def process_one_file(infile):

    outfile = infile.parent / build_output_filename(infile.name)

    if outfile.exists() and not overwrite:
        print(f"Skipping existing: {outfile.name}")
        return

    if outfile.exists() and overwrite:
        outfile.unlink()

    print(f"Reading : {infile.name}")
    print(f"Writing : {outfile.name}")

    with xr.open_dataset(infile, decode_times=False) as ds:

        ds_out = rename_variable_in_dataset(ds)
        ds_out.to_netcdf(outfile)


# ============================================================
# 4) main
# ============================================================

def main():

    files = find_input_files(input_dir, file_pattern)

    if len(files) == 0:
        raise FileNotFoundError("No files found.")

    print(f"Found {len(files)} PRECT files")

    # limit number of files if requested
    if max_files is not None:
        files = files[:max_files]
        print(f"Processing first {len(files)} files")

    processed = 0

    for f in files:

        try:
            process_one_file(f)
            processed += 1

        except Exception as exc:
            print(f"Failed: {f.name}")
            print(exc)

    print()
    print("Finished.")
    print(f"Processed {processed} files")


if __name__ == "__main__":
    main()
