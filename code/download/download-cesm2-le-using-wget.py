#!/usr/bin/env python3

# Download CESM2-LE data one file at a time and subset to a geographic region

import re
import time
import subprocess
from pathlib import Path
from collections import defaultdict

import numpy as np
import xarray as xr

from livio_intern_project.master_file_list_CESM2_LE_PRECT import master_file_list
from livio_intern_project import config, misc


# input ------------------------------
variable   = "PRECT"
years      = np.arange(1850, 1860, 1)
n_members  = 2

outdir     = Path(config.dirs["raw"]) 

# geographical bounds
lon_min    = 2.0
lon_max    = 32.5
lat_min    = 53.0
lat_max    = 73.5

overwrite  = True
remove_tmp = True

# retry settings
max_download_attempts = 5
retry_wait_seconds    = 20
# ------------------------------------


def parse_filename_info(path_or_url):
    """
    Extract variable and time span from a CESM2-LE filename.
    """
    fname = Path(path_or_url).name

    pattern = re.compile(
        r"\.cam\.h1\.(?P<variable>[A-Za-z0-9_]+)\.(?P<start>\d{8})-(?P<end>\d{8})\.nc$"
    )
    m = pattern.search(fname)
    if m is None:
        return None

    start = m.group("start")
    end = m.group("end")

    return {
        "variable": m.group("variable"),
        "start_year": int(start[:4]),
        "end_year": int(end[:4]),
        "time_span": f"{start}-{end}",
        "basename": fname,
    }


def subset_cesm2le_files(variable=None, years=None):
    """
    Select files from master_file_list by variable and overlapping years.
    """
    years = None if years is None else np.asarray(years)

    selected = []
    for url in master_file_list:
        info = parse_filename_info(url)
        if info is None:
            continue

        if variable is not None and info["variable"] != variable:
            continue

        if years is not None:
            overlap = np.any((years >= info["start_year"]) & (years <= info["end_year"]))
            if not overlap:
                continue

        selected.append(url)

    return sorted(selected)


def select_first_members_per_group(file_list, n_members=None):
    """
    Keep the first n_members files for each (variable, time_span) group.
    """
    groups = defaultdict(list)

    for url in file_list:
        info = parse_filename_info(url)
        if info is None:
            continue
        groups[(info["variable"], info["time_span"])].append(url)

    selected = []
    for key in sorted(groups):
        files = sorted(groups[key])
        if n_members is not None:
            files = files[:n_members]
        selected.extend(files)

    return selected


def simplified_filename_list(file_list):
    """
    Create simplified output filenames:
    cam.{variable}.{ensemble}.{time_span}.nc
    """
    groups = defaultdict(list)

    for url in file_list:
        info = parse_filename_info(url)
        if info is None:
            continue
        groups[(info["variable"], info["time_span"])].append(url)

    new_names = []
    for key in sorted(groups):
        files = sorted(groups[key])
        variable, time_span = key
        for i, _ in enumerate(files, start=1):
            new_names.append(f"cam.{variable}.{i:03d}.{time_span}.nc")

    return new_names


def download_one_file(url, destination, overwrite=False,
                      max_attempts=5, wait_seconds=20):
    """
    Download one file with wget, retrying if the server fails.
    """
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and overwrite:
        destination.unlink()

    if destination.exists() and not overwrite:
        print(f"Raw file exists, skipping download: {destination}")
        return destination

    cmd = [
        "wget",
        "-c",
        "--tries=1",   # we do retries ourselves
        "-O", str(destination),
        url,
    ]

    for attempt in range(1, max_attempts + 1):
        print(f"Download attempt {attempt}/{max_attempts}")
        print(" ".join(cmd))

        result = subprocess.run(cmd)

        if result.returncode == 0 and destination.exists() and destination.stat().st_size > 0:
            return destination

        print(f"Download failed for {url}")

        if destination.exists():
            destination.unlink()

        if attempt < max_attempts:
            print(f"Waiting {wait_seconds} seconds before retrying...")
            time.sleep(wait_seconds)

    raise RuntimeError(f"Download failed after {max_attempts} attempts: {url}")


def _find_coord_name(ds, names):
    for name in names:
        if name in ds.coords or name in ds.dims or name in ds.variables:
            return name
    return None


def subset_geographical_area(infile, outfile, lon_min, lon_max, lat_min, lat_max):
    """
    Read one NetCDF file, subset it to the requested lat/lon box, and write output.
    """
    infile = Path(infile)
    outfile = Path(outfile)
    outfile.parent.mkdir(parents=True, exist_ok=True)

    with xr.open_dataset(infile) as ds:
        lon_name = _find_coord_name(ds, ["lon", "longitude", "LON", "LONGITUDE"])
        lat_name = _find_coord_name(ds, ["lat", "latitude", "LAT", "LATITUDE"])

        if lon_name is None or lat_name is None:
            raise ValueError(f"Could not find lat/lon coordinates in {infile}")

        lon = ds[lon_name]
        lat = ds[lat_name]

        if lon.ndim == 1 and lat.ndim == 1:
            lon_slice = slice(lon_min, lon_max) if lon.values[0] < lon.values[-1] else slice(lon_max, lon_min)
            lat_slice = slice(lat_min, lat_max) if lat.values[0] < lat.values[-1] else slice(lat_max, lat_min)
            ds_sub = ds.sel({lon_name: lon_slice, lat_name: lat_slice})

        elif lon.ndim == 2 and lat.ndim == 2:
            mask = (
                (lon >= lon_min) & (lon <= lon_max) &
                (lat >= lat_min) & (lat <= lat_max)
            )
            ds_sub = ds.where(mask, drop=True)

        else:
            raise ValueError(f"Unsupported lat/lon coordinate structure in {infile}")

        print(f"Writing subset: {outfile}")
        ds_sub.to_netcdf(outfile)


def download_and_subset_one_file(url, new_filename, outdir,
                                 lon_min, lon_max, lat_min, lat_max,
                                 overwrite=False, remove_tmp=True):
    """
    Download one raw file, subset it, write the final file, and optionally
    remove the raw file.
    """
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    raw_path = outdir / Path(url).name
    final_path = outdir / new_filename

    if final_path.exists() and not overwrite:
        print(f"Final file exists, skipping: {final_path}")
        return final_path

    download_one_file(
        url,
        raw_path,
        overwrite=overwrite,
        max_attempts=max_download_attempts,
        wait_seconds=retry_wait_seconds,
    )

    try:
        subset_geographical_area(
            raw_path,
            final_path,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
        )
    except Exception:
        if final_path.exists():
            final_path.unlink()
        raise

    if remove_tmp and raw_path.exists():
        raw_path.unlink()

    return final_path


if __name__ == "__main__":

    files = subset_cesm2le_files(variable=variable,years=years)

    files = select_first_members_per_group(files,n_members=n_members)

    new_files = simplified_filename_list(files)

    print(f"Number of files selected: {len(files)}")
    print(f"Output directory: {outdir}")
    print(f"Subset bounds: lon {lon_min} to {lon_max}, lat {lat_min} to {lat_max}")

    for url, new_name in zip(files, new_files):

        misc.tic()
        
        print()
        print(f"Processing: {url}")
        print(f"Output name: {new_name}")

        try:
            download_and_subset_one_file(
                url=url,
                new_filename=new_name,
                outdir=outdir,
                lon_min=lon_min,
                lon_max=lon_max,
                lat_min=lat_min,
                lat_max=lat_max,
                overwrite=overwrite,
                remove_tmp=remove_tmp,
            )
        except Exception as e:
            print(f"Failed for {url}")
            print(f"Reason: {e}")

        misc.toc()
