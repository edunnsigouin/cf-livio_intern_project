"""
Interpolate daily ERA5 data to the CESM2-LE grid using xESMF.

Method:
    ERA5 variables are first converted to units comparable with CESM2-LE,
    then interpolated one yearly file at a time to the CESM2-LE grid.
    Conservative interpolation is used for precipitation and snow water
    equivalent because these represent water amounts that should be
    approximately conserved during remapping. Bilinear interpolation is used
    for soil moisture because it is treated as a local land-surface state
    variable, where smooth spatial variation is usually more important than
    exact conservation. For land-only variables, the ERA5 land-sea mask is used
    to prevent ocean grid cells from influencing land-variable interpolation
    near coastlines.

Supported variables:
    tp    : total precipitation
    sd    : snow depth / snow water equivalent
    swvl1 : volumetric soil water layer 1

Outputs:
    - keep original ERA5 variable names: tp, sd, swvl1
    - are interpolated to the CESM2-LE grid
    - use units comparable to CESM2-LE
    - do not write xESMF weights to file

NOTE: due to issues with the latest version of python, to run this script,
use the 'regrid' conda environment and not 'geo_scipy'.
"""

# ====================================================
# 1. Important statements
# ====================================================

import os
import glob
import xarray as xr
import numpy as np
import xesmf as xe

from livio_intern_project import config


# ====================================================
# 2. User input parameters and paths
# ====================================================

variable = "swvl1"  # choose "tp", "sd", or "swvl1"
years = np.arange(2020, 2021, 1)

path_in_era5_base = config.dirs["raw_era5_daily"]
path_in_cesm = config.dirs["raw_cesm2_le"]
path_out_base = config.dirs["interpolated_era5_daily"]

land_sea_mask_file = os.path.join(
    path_in_era5_base,
    "era5_land_sea_mask_0.25x0.25_scandinavia.nc",
)

write2file = True


# ====================================================
# 3. Functions
# ====================================================

def get_variable_info(variable):
    """
    Lookup table for ERA5 and CESM2-LE variable names,
    output units, interpolation method, and masking.
    """

    lookup = {
        "tp": {
            "era5_name": "tp",
            "cesm_name": "PRECT",
            "method": "conservative",
            "output_units": "mm/day",
            "long_name": "Total daily precipitation",
            "use_land_mask": False,
        },
        "sd": {
            "era5_name": "sd",
            "cesm_name": "SWE",
            "method": "conservative",
            "output_units": "kg/m^2",
            "long_name": "Snow water equivalent",
            "use_land_mask": True,
        },
        "swvl1": {
            "era5_name": "swvl1",
            "cesm_name": "SM",
            "method": "bilinear",
            "output_units": "kg/m^2",
            "long_name": "Soil moisture layer 1 water mass",
            "use_land_mask": True,
        },
    }

    if variable not in lookup:
        raise ValueError(f"Variable '{variable}' is not defined.")

    return lookup[variable]


def find_file(pattern):
    """Find one file matching a pattern."""

    files = sorted(glob.glob(pattern, recursive=True))

    if len(files) == 0:
        raise FileNotFoundError(f"No files found with pattern:\n{pattern}")

    return files[0]


def get_era5_file(year, path_in, variable):
    """Return expected ERA5 yearly file."""

    pattern = os.path.join(path_in, f"{variable}_0.25x0.25_{year}*.nc")
    files = sorted(glob.glob(pattern))

    if len(files) == 0:
        raise FileNotFoundError(f"No ERA5 file found for {year}:\n{pattern}")

    if len(files) > 1:
        raise ValueError(f"More than one ERA5 file found for {year}:\n{files}")

    return files[0]


def prepare_era5_grid(ds):
    """Rename ERA5 coordinates and sort latitude south-to-north."""

    rename_dict = {}

    if "latitude" in ds.coords:
        rename_dict["latitude"] = "lat"

    if "longitude" in ds.coords:
        rename_dict["longitude"] = "lon"

    if rename_dict:
        ds = ds.rename(rename_dict)

    if ds.lat[0] > ds.lat[-1]:
        ds = ds.sortby("lat")

    return ds


def prepare_cesm_grid(ds):
    """Keep only CESM2-LE grid coordinates needed by xESMF."""

    return ds[["lat", "lon"]]


def normalise_units(units):
    """Make unit strings easier to compare."""

    if units is None:
        return ""

    return (
        units.strip()
        .lower()
        .replace(" ", "")
        .replace("−", "-")
        .replace("_", "")
    )


def convert_tp_to_mm_per_day(da):
    """Convert ERA5 tp to mm/day if needed."""

    original_units = da.attrs.get("units", "")
    units_clean = normalise_units(original_units)

    if units_clean in ["m", "meter", "metre", "meters", "metres"]:
        da = da * 1000.0
        note = "Converted from m to mm/day using factor 1000."

    elif units_clean in ["mm", "mm/day", "mmday-1", "mmd-1", "mmperday"]:
        note = "No conversion applied; input already treated as mm/day."

    else:
        raise ValueError(f"Unknown tp units: '{original_units}'.")

    da.attrs["units"] = "mm/day"
    da.attrs["original_units"] = original_units
    da.attrs["conversion_note"] = note

    return da


def convert_sd_to_kg_per_m2(da):
    """Convert ERA5 sd from m water equivalent to kg/m^2."""

    original_units = da.attrs.get("units", "")
    units_clean = normalise_units(original_units)

    if units_clean in [
        "m",
        "meter",
        "metre",
        "meters",
        "metres",
        "mofwaterequivalent",
    ]:
        da = da * 1000.0
        note = "Converted from m water equivalent to kg/m^2 using factor 1000."

    elif units_clean in ["kg/m2", "kg/m^2", "kgm-2"]:
        note = "No conversion applied; input already kg/m^2."

    else:
        raise ValueError(f"Unknown sd units: '{original_units}'.")

    da.attrs["units"] = "kg/m^2"
    da.attrs["original_units"] = original_units
    da.attrs["conversion_note"] = note

    return da


def convert_swvl1_to_kg_per_m2(da):
    """
    Convert ERA5 swvl1 from volumetric soil water to kg/m^2.

    ERA5 swvl1:
        0-7 cm soil layer
        units m^3/m^3

    Conversion:
        swvl1 * layer thickness * water density
    """

    original_units = da.attrs.get("units", "")
    units_clean = normalise_units(original_units)

    if units_clean in [
        "m^3m^-3",
        "m3m-3",
        "m**3m**-3",
        "m3/m3",
    ]:
        layer_thickness_m = 0.07
        water_density = 1000.0

        da = da * layer_thickness_m * water_density
        note = (
            "Converted from m^3/m^3 to kg/m^2 using "
            "ERA5 layer 1 thickness 0.07 m and water density 1000 kg/m^3."
        )

    elif units_clean in ["kg/m2", "kg/m^2", "kgm-2"]:
        note = "No conversion applied; input already kg/m^2."

    else:
        raise ValueError(f"Unknown swvl1 units: '{original_units}'.")

    da.attrs["units"] = "kg/m^2"
    da.attrs["original_units"] = original_units
    da.attrs["conversion_note"] = note

    return da


def convert_units_if_needed(da, info):
    """Apply variable-specific unit conversion."""

    if info["era5_name"] == "tp":
        return convert_tp_to_mm_per_day(da)

    if info["era5_name"] == "sd":
        return convert_sd_to_kg_per_m2(da)

    if info["era5_name"] == "swvl1":
        return convert_swvl1_to_kg_per_m2(da)

    raise NotImplementedError(f"No unit conversion defined for {info['era5_name']}.")


def load_era5_land_mask(mask_file, reference_grid):
    """
    Load ERA5 land-sea mask and convert it to xESMF convention.

    ERA5 mask:
        0 = water
        1 = land
        fractional values near coasts

    xESMF mask:
        1 = valid
        0 = masked

    Here:
        land is mask > 0.5
    """

    if not os.path.exists(mask_file):
        raise FileNotFoundError(f"Land-sea mask file not found:\n{mask_file}")

    ds_mask = xr.open_dataset(mask_file)
    ds_mask = prepare_era5_grid(ds_mask)

    if "lsm" in ds_mask:
        mask_var = "lsm"
    elif "land_sea_mask" in ds_mask:
        mask_var = "land_sea_mask"
    else:
        raise ValueError("Could not find 'lsm' or 'land_sea_mask' in mask file.")

    lsm = ds_mask[mask_var]

    for dim in ["time", "valid_time"]:
        if dim in lsm.dims:
            lsm = lsm.isel({dim: 0}, drop=True)

    lsm = lsm.squeeze(drop=True)

    lsm = lsm.sel(
        lat=reference_grid.lat,
        lon=reference_grid.lon,
    )

    mask = xr.where(lsm > 0.5, 1, 0).astype("int32")
    mask.name = "mask"
    mask = mask.transpose("lat", "lon")

    return mask


def build_source_grid(ds_era5, info):
    """Create ERA5 source grid, with land mask if required."""

    ds_grid = ds_era5[["lat", "lon"]].copy()

    if info["use_land_mask"]:
        source_mask = load_era5_land_mask(
            mask_file=land_sea_mask_file,
            reference_grid=ds_era5,
        )
        ds_grid["mask"] = source_mask

    return ds_grid


def build_regridder(ds_in, ds_out, method):
    """Build an xESMF regridder without writing weights."""

    return xe.Regridder(
        ds_in,
        ds_out,
        method=method,
    )


def interpolate_one_year(year, info, ds_cesm_grid):
    """Open, convert, optionally mask, interpolate, and return one year."""

    path_in_era5 = os.path.join(path_in_era5_base, info["era5_name"])
    era5_file = get_era5_file(year, path_in_era5, info["era5_name"])

    print(f"Processing {info['era5_name']} {year}")
    print(f"ERA5 input: {era5_file}")

    ds_era5 = xr.open_dataset(era5_file)
    ds_era5 = prepare_era5_grid(ds_era5)

    da = ds_era5[info["era5_name"]]
    da = convert_units_if_needed(da, info)

    ds_era5_grid = build_source_grid(ds_era5, info)

    if info["use_land_mask"]:
        da = da.where(ds_era5_grid["mask"] == 1)

    regridder = build_regridder(
        ds_in=ds_era5_grid,
        ds_out=ds_cesm_grid,
        method=info["method"],
    )

    da_interp = regridder(da)

    da_interp.name = info["era5_name"]
    da_interp.attrs["units"] = info["output_units"]
    da_interp.attrs["long_name"] = (
        f"ERA5 {info['long_name']} interpolated to CESM2-LE grid"
    )
    da_interp.attrs["interpolation_method"] = info["method"]
    da_interp.attrs["source_variable"] = info["era5_name"]
    da_interp.attrs["target_grid_variable"] = info["cesm_name"]
    da_interp.attrs["target_grid"] = "CESM2-LE"

    if info["use_land_mask"]:
        da_interp.attrs["land_sea_mask"] = "ERA5 land_sea_mask > 0.5"
        da_interp.attrs["land_sea_mask_file"] = land_sea_mask_file

    ds_out = da_interp.to_dataset()

    keep_vars = [
        info["era5_name"],
        "time",
        "lat",
        "lon",
    ]

    ds_out = ds_out[keep_vars]

    drop_coords = []

    for coord in ["number", "expver"]:
        if coord in ds_out.coords:
            drop_coords.append(coord)

    if len(drop_coords) > 0:
        ds_out = ds_out.drop_vars(drop_coords)

    return ds_out


def write_output(ds, year, info):
    """Write interpolated yearly file."""

    path_out = os.path.join(path_out_base, info["era5_name"])
    os.makedirs(path_out, exist_ok=True)

    file_out = os.path.join(
        path_out,
        f"{info['era5_name']}_era5_interpolated_to_cesm2-le_{year}.nc",
    )

    print(f"Writing: {file_out}")
    ds.to_netcdf(file_out)

    return file_out


# ====================================================
# 4. Main script
# ====================================================

if __name__ == "__main__":

    info = get_variable_info(variable)

    cesm_grid_file_pattern = path_in_cesm + f"/**/*{info['cesm_name']}*.nc"

    cesm_grid_file = find_file(cesm_grid_file_pattern)
    print(f"Using CESM2-LE grid from: {cesm_grid_file}")

    ds_cesm = xr.open_dataset(cesm_grid_file)
    ds_cesm_grid = prepare_cesm_grid(ds_cesm)

    for year in years:

        ds_interp = interpolate_one_year(
            year=year,
            info=info,
            ds_cesm_grid=ds_cesm_grid,
        )

        if write2file:
            write_output(ds_interp, year, info)
        else:
            print(ds_interp)
