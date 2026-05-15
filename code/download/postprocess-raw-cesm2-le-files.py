#!/usr/bin/env python3

"""
Clean, subset, derive, convert, and rename CESM2-LE NetCDF files.

Supported variable types:

1. Standard variables
   Example:
       variable = "PRECT"

2. Derived variables
   SWE = SNOWLIQ + SNOWICE
   SM  = TOTSOILLIQ + TOTSOILICE

Output filename format:
    cesm2-le.{variable}.{ensemble_number}.{time_period}.nc
"""

import re
from pathlib import Path
from collections import defaultdict

import xarray as xr
from livio_intern_project import config


# ============================================================
# 1. User settings
# ============================================================

variable = "SM"

input_dir = Path(config.dirs["raw_cesm2_le"] + variable + "/")
output_dir = input_dir

file_pattern = "*.nc"

replace_original_files = True
overwrite = True

expected_files_per_period = 100

selected_time_periods = [
    "19300101-19391231",
    "19400101-19491231",
    "19500101-19591231",
    "19600101-19691231",
    "19700101-19791231",
    "19800101-19891231",
    "19900101-19991231",
    "20000101-20091231",
    "20100101-20141231",
    "20150101-20241231",
    "20250101-20341231",
]

lon_min = 2.0
lon_max = 32.5
lat_min = 53.0
lat_max = 73.5


# ============================================================
# 2. Derived-variable definitions
# ============================================================

DERIVED_VARIABLES = {
    "SWE": {
        "components": ["SNOWLIQ", "SNOWICE"],
        "long_name": "Snow water equivalent",
    },
    "SM": {
        "components": ["TOTSOILLIQ", "TOTSOILICE"],
        "long_name": "Total soil moisture",
    },
}


# ============================================================
# 3. Fixed CESM2-LE ensemble-member mapping
# ============================================================

CESM2_LE_MEMBERS = [
    "LE2-1001.001", "LE2-1011.001",
    "LE2-1021.002", "LE2-1031.002",
    "LE2-1041.003", "LE2-1051.003",
    "LE2-1061.004", "LE2-1071.004",
    "LE2-1081.005", "LE2-1091.005",
    "LE2-1101.006", "LE2-1111.006",
    "LE2-1121.007", "LE2-1131.007",
    "LE2-1141.008", "LE2-1151.008",
    "LE2-1161.009", "LE2-1171.009",
    "LE2-1181.010", "LE2-1191.010",

    *[f"LE2-1231.{i:03d}" for i in range(1, 21)],
    *[f"LE2-1251.{i:03d}" for i in range(1, 21)],
    *[f"LE2-1281.{i:03d}" for i in range(1, 21)],
    *[f"LE2-1301.{i:03d}" for i in range(1, 21)],
]

CESM2_LE_MEMBER_TO_NUMBER = {
    member_id: number
    for number, member_id in enumerate(CESM2_LE_MEMBERS, start=1)
}


# ============================================================
# 4. Variable helpers
# ============================================================

def is_derived_variable(variable):
    return variable in DERIVED_VARIABLES


def get_primary_component(variable):
    return DERIVED_VARIABLES[variable]["components"][0]


def get_secondary_component(variable):
    return DERIVED_VARIABLES[variable]["components"][1]


# ============================================================
# 5. Filename and ensemble-member helpers
# ============================================================

def get_time_period_from_filename(filename):
    match = re.search(r"(\d{8}-\d{8})\.nc$", filename)

    if match is None:
        return None

    return match.group(1)


def get_cesm2_member_id_from_filename(filename):
    match = re.search(r"(LE2-\d{4}\.\d{3})", filename)

    if match is None:
        raise ValueError(f"Could not extract CESM2-LE member ID from filename: {filename}")

    return match.group(1)


def get_fixed_ensemble_number(filename):
    member_id = get_cesm2_member_id_from_filename(filename)

    if member_id not in CESM2_LE_MEMBER_TO_NUMBER:
        raise ValueError(f"Unknown CESM2-LE member ID: {member_id}")

    return CESM2_LE_MEMBER_TO_NUMBER[member_id]


def make_processed_filename(variable, ensemble_number, time_period):
    return f"cesm2-le.{variable}.{ensemble_number:03d}.{time_period}.nc"


def make_matching_component_file(primary_file, primary_component, secondary_component):
    primary_file = Path(primary_file)

    secondary_file = primary_file.with_name(
        primary_file.name.replace(
            f".{primary_component}.",
            f".{secondary_component}.",
        )
    )

    if secondary_file == primary_file:
        raise ValueError(
            f"Input file does not contain .{primary_component}.: {primary_file}"
        )

    if not secondary_file.exists():
        raise FileNotFoundError(f"Matching component file not found: {secondary_file}")

    return secondary_file


# ============================================================
# 6. File discovery and grouping
# ============================================================

def find_input_files(directory, pattern, variable):
    files = sorted(Path(directory).glob(pattern))

    if is_derived_variable(variable):
        primary_component = get_primary_component(variable)

        files = [
            file_path for file_path in files
            if f".{primary_component}." in file_path.name
        ]

    return files


def group_files_by_time_period(files):
    grouped_files = defaultdict(list)

    for file_path in files:
        time_period = get_time_period_from_filename(file_path.name)

        if time_period is None:
            print(f"Skipping file with unrecognized time period: {file_path.name}")
            continue

        grouped_files[time_period].append(file_path)

    return {
        time_period: sorted(file_list)
        for time_period, file_list in grouped_files.items()
    }


def keep_only_selected_time_periods(grouped_files, selected_time_periods):
    if selected_time_periods is None:
        return grouped_files

    selected_groups = {}

    for time_period in selected_time_periods:
        if time_period not in grouped_files:
            print(f"Warning: requested time period not found: {time_period}")
            continue

        selected_groups[time_period] = grouped_files[time_period]

    return selected_groups


# ============================================================
# 7. Dataset helpers
# ============================================================

def remove_invalid_time_bounds_attribute(dataset):
    if "time" in dataset and "bounds" in dataset["time"].attrs:
        bounds_name = dataset["time"].attrs["bounds"]

        if bounds_name not in dataset.variables:
            del dataset["time"].attrs["bounds"]

    return dataset


def keep_variable_lat_lon_time(dataset, variable):
    required_names = [variable, "lat", "lon", "time"]

    missing = [
        name for name in required_names
        if name not in dataset.variables and name not in dataset.coords
    ]

    if missing:
        raise ValueError(f"Missing required variables or coordinates: {missing}")

    existing_names = [
        name for name in required_names
        if name in dataset.variables or name in dataset.coords
    ]

    cleaned = dataset[existing_names].copy()

    coord_names = [
        name for name in ["lat", "lon", "time"]
        if name in cleaned
    ]

    cleaned = cleaned.set_coords(coord_names)
    cleaned = remove_invalid_time_bounds_attribute(cleaned)

    return cleaned


def subset_to_geographical_bounds(dataset, lon_min, lon_max, lat_min, lat_max):
    if "lon" not in dataset or "lat" not in dataset:
        raise ValueError("Dataset must contain 'lat' and 'lon'.")

    lon = dataset["lon"]
    lat = dataset["lat"]

    if lon.ndim == 1 and lat.ndim == 1:
        lon_slice = (
            slice(lon_min, lon_max)
            if lon.values[0] < lon.values[-1]
            else slice(lon_max, lon_min)
        )

        lat_slice = (
            slice(lat_min, lat_max)
            if lat.values[0] < lat.values[-1]
            else slice(lat_max, lat_min)
        )

        return dataset.sel(lon=lon_slice, lat=lat_slice)

    if lon.ndim == 2 and lat.ndim == 2:
        mask = (
            (lon >= lon_min) & (lon <= lon_max) &
            (lat >= lat_min) & (lat <= lat_max)
        )

        return dataset.where(mask, drop=True)

    raise ValueError("Unsupported lat/lon coordinate structure.")


def convert_prect_units_if_needed(dataset, variable):
    if variable != "PRECT":
        return dataset

    original_attrs = dataset["PRECT"].attrs.copy()

    dataset["PRECT"] = dataset["PRECT"] * 86400000.0

    dataset["PRECT"].attrs = original_attrs
    dataset["PRECT"].attrs["units"] = "mm/day"
    dataset["PRECT"].attrs["long_name"] = "Total daily precipitation"
    dataset["PRECT"].attrs["conversion_note"] = (
        "Converted from m/s to mm/day using factor 86400000."
    )

    return dataset


def add_processing_metadata(
    dataset,
    variable,
    processed_filename,
    cesm2_member_id,
    ensemble_number,
    source_original_filename=None,
    component_filenames=None,
):
    dataset.attrs["processed_filename"] = processed_filename
    dataset.attrs["cesm2_le_member_id"] = cesm2_member_id
    dataset.attrs["cesm2_le_ensemble_number"] = f"{ensemble_number:03d}"
    dataset.attrs["variable"] = variable

    if source_original_filename is not None:
        dataset.attrs["source_original_filename"] = source_original_filename

    if component_filenames is not None:
        for component_name, filename in component_filenames.items():
            attr_name = f"source_{component_name.lower()}_filename"
            dataset.attrs[attr_name] = filename

    dataset.attrs["processing_note"] = (
        "File cleaned, geographically subset, renamed, and linked to fixed CESM2-LE ensemble mapping."
    )

    return dataset


# ============================================================
# 8. Standard variable processing
# ============================================================

def process_standard_variable_file(
    input_file,
    output_file,
    variable,
    lon_min,
    lon_max,
    lat_min,
    lat_max,
):
    with xr.open_dataset(input_file, decode_times=False) as dataset:
        processed = keep_variable_lat_lon_time(dataset, variable)

        processed = subset_to_geographical_bounds(
            processed,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
        )

        processed = convert_prect_units_if_needed(
            processed,
            variable=variable,
        )

        cesm2_member_id = get_cesm2_member_id_from_filename(input_file.name)
        ensemble_number = get_fixed_ensemble_number(input_file.name)

        processed = add_processing_metadata(
            dataset=processed,
            variable=variable,
            processed_filename=output_file.name,
            cesm2_member_id=cesm2_member_id,
            ensemble_number=ensemble_number,
            source_original_filename=input_file.name,
        )

        processed.to_netcdf(output_file)


# ============================================================
# 9. Derived variable processing
# ============================================================

def check_component_file_contents(
    primary_dataset,
    secondary_dataset,
    primary_file,
    secondary_file,
    primary_component,
    secondary_component,
):
    required_primary_names = [primary_component, "lat", "lon", "time"]
    required_secondary_names = [secondary_component, "lat", "lon", "time"]

    missing_primary = [
        name for name in required_primary_names
        if name not in primary_dataset.variables and name not in primary_dataset.coords
    ]

    missing_secondary = [
        name for name in required_secondary_names
        if name not in secondary_dataset.variables and name not in secondary_dataset.coords
    ]

    if missing_primary:
        raise ValueError(f"Missing {missing_primary} in {primary_file}")

    if missing_secondary:
        raise ValueError(f"Missing {missing_secondary} in {secondary_file}")


def create_derived_variable_dataset(
    primary_dataset,
    secondary_dataset,
    variable,
    primary_component,
    secondary_component,
):
    derived = primary_dataset[primary_component] + secondary_dataset[secondary_component]

    derived.attrs = primary_dataset[primary_component].attrs.copy()
    derived.attrs["long_name"] = DERIVED_VARIABLES[variable]["long_name"]
    derived.attrs["derived_from"] = f"{primary_component} + {secondary_component}"

    dataset = xr.Dataset(
        data_vars={variable: derived},
        coords={
            "time": primary_dataset["time"],
            "lat": primary_dataset["lat"],
            "lon": primary_dataset["lon"],
        },
        attrs=primary_dataset.attrs,
    )

    dataset = remove_invalid_time_bounds_attribute(dataset)

    return dataset


def process_derived_variable_file(
    primary_file,
    output_file,
    variable,
    lon_min,
    lon_max,
    lat_min,
    lat_max,
):
    primary_component = get_primary_component(variable)
    secondary_component = get_secondary_component(variable)

    secondary_file = make_matching_component_file(
        primary_file=primary_file,
        primary_component=primary_component,
        secondary_component=secondary_component,
    )

    print(f"Reading {variable} components:")
    print(f"  {primary_component}: {primary_file.name}")
    print(f"  {secondary_component}: {secondary_file.name}")

    with xr.open_dataset(primary_file, decode_times=False) as primary_dataset, \
         xr.open_dataset(secondary_file, decode_times=False) as secondary_dataset:

        check_component_file_contents(
            primary_dataset=primary_dataset,
            secondary_dataset=secondary_dataset,
            primary_file=primary_file,
            secondary_file=secondary_file,
            primary_component=primary_component,
            secondary_component=secondary_component,
        )

        processed = create_derived_variable_dataset(
            primary_dataset=primary_dataset,
            secondary_dataset=secondary_dataset,
            variable=variable,
            primary_component=primary_component,
            secondary_component=secondary_component,
        )

        processed = subset_to_geographical_bounds(
            processed,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
        )

        cesm2_member_id = get_cesm2_member_id_from_filename(primary_file.name)
        ensemble_number = get_fixed_ensemble_number(primary_file.name)

        processed = add_processing_metadata(
            dataset=processed,
            variable=variable,
            processed_filename=output_file.name,
            cesm2_member_id=cesm2_member_id,
            ensemble_number=ensemble_number,
            source_original_filename=primary_file.name,
            component_filenames={
                primary_component: primary_file.name,
                secondary_component: secondary_file.name,
            },
        )

        processed.to_netcdf(output_file)


# ============================================================
# 10. Write-control wrapper
# ============================================================

def process_one_file_and_write_output(
    input_file,
    output_file,
    variable,
    lon_min,
    lon_max,
    lat_min,
    lat_max,
    overwrite,
):
    input_file = Path(input_file)
    output_file = Path(output_file)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_file.exists() and not overwrite:
        print(f"Output exists, skipping: {output_file}")
        return output_file

    if output_file.exists() and overwrite:
        output_file.unlink()

    cesm2_member_id = get_cesm2_member_id_from_filename(input_file.name)
    ensemble_number = get_fixed_ensemble_number(input_file.name)

    print(f"Reading : {input_file}")
    print(f"Writing : {output_file}")
    print(f"Member  : {cesm2_member_id} -> {ensemble_number:03d}")

    if is_derived_variable(variable):
        process_derived_variable_file(
            primary_file=input_file,
            output_file=output_file,
            variable=variable,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
        )
    else:
        process_standard_variable_file(
            input_file=input_file,
            output_file=output_file,
            variable=variable,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
        )

    return output_file


# ============================================================
# 11. Processing one time-period group
# ============================================================

def process_all_files_in_time_period(
    files,
    time_period,
    variable,
    output_dir,
    lon_min,
    lon_max,
    lat_min,
    lat_max,
    overwrite,
    replace_original_files,
):
    for input_file in files:
        ensemble_number = get_fixed_ensemble_number(input_file.name)

        processed_filename = make_processed_filename(
            variable=variable,
            ensemble_number=ensemble_number,
            time_period=time_period,
        )

        print(f"{input_file.name} -> {processed_filename}")

        if replace_original_files:
            temporary_file = input_file.parent / f"tmp_{processed_filename}"
            final_file = input_file.parent / processed_filename

            process_one_file_and_write_output(
                input_file=input_file,
                output_file=temporary_file,
                variable=variable,
                lon_min=lon_min,
                lon_max=lon_max,
                lat_min=lat_min,
                lat_max=lat_max,
                overwrite=overwrite,
            )

            if input_file.resolve() != final_file.resolve():
                input_file.unlink(missing_ok=True)

            if final_file.exists():
                if overwrite:
                    final_file.unlink()
                else:
                    raise FileExistsError(f"Final file already exists: {final_file}")

            temporary_file.rename(final_file)

        else:
            output_file = Path(output_dir) / processed_filename

            process_one_file_and_write_output(
                input_file=input_file,
                output_file=output_file,
                variable=variable,
                lon_min=lon_min,
                lon_max=lon_max,
                lat_min=lat_min,
                lat_max=lat_max,
                overwrite=overwrite,
            )

    print(f"Finished time period {time_period}: {len(files)} files")


# ============================================================
# 12. Reporting helpers
# ============================================================

def print_detected_time_periods(grouped_files, expected_files_per_period, variable):
    print("\nDetected time periods:")

    for time_period in sorted(grouped_files):
        n_files = len(grouped_files[time_period])

        if is_derived_variable(variable):
            primary_component = get_primary_component(variable)
            print(f"  {time_period}: {n_files} {primary_component} files")
        else:
            print(f"  {time_period}: {n_files} files")

        if expected_files_per_period is not None and n_files != expected_files_per_period:
            print(
                f"  WARNING: expected {expected_files_per_period} files, "
                f"but found {n_files}"
            )


def print_selected_files_to_process(grouped_files, variable):
    print("\nSelected files to process:")

    for time_period in sorted(grouped_files):
        files = grouped_files[time_period]
        print(f"  {time_period}: {len(files)} files")

        for file_path in files[:5]:
            member_id = get_cesm2_member_id_from_filename(file_path.name)
            ensemble_number = get_fixed_ensemble_number(file_path.name)

            if is_derived_variable(variable):
                primary_component = get_primary_component(variable)
                secondary_component = get_secondary_component(variable)

                secondary_file = make_matching_component_file(
                    primary_file=file_path,
                    primary_component=primary_component,
                    secondary_component=secondary_component,
                )

                print(
                    f"     {file_path.name} + {secondary_file.name} "
                    f"-> ensemble {ensemble_number:03d} ({member_id})"
                )
            else:
                print(
                    f"     {file_path.name} "
                    f"-> ensemble {ensemble_number:03d} ({member_id})"
                )

        if len(files) > 5:
            print("     ...")


# ============================================================
# 13. Main script
# ============================================================

if __name__ == "__main__":

    print("\nStarting CESM2-LE processing")
    print(f"Variable: {variable}")
    print(f"Input directory: {input_dir}")

    all_files = find_input_files(
        directory=input_dir,
        pattern=file_pattern,
        variable=variable,
    )

    if len(all_files) == 0:
        raise FileNotFoundError(
            f"No input files found in {input_dir} for variable {variable}"
        )

    print(f"\nFound {len(all_files)} primary NetCDF files.")

    files_by_time_period = group_files_by_time_period(all_files)

    print_detected_time_periods(
        grouped_files=files_by_time_period,
        expected_files_per_period=expected_files_per_period,
        variable=variable,
    )

    files_to_process = keep_only_selected_time_periods(
        grouped_files=files_by_time_period,
        selected_time_periods=selected_time_periods,
    )

    if len(files_to_process) == 0:
        raise ValueError("No files left to process after selecting time periods.")

    print_selected_files_to_process(
        grouped_files=files_to_process,
        variable=variable,
    )

    if not replace_original_files:
        output_dir.mkdir(parents=True, exist_ok=True)

    for time_period in sorted(files_to_process):
        print()
        print(f"Processing time period: {time_period}")

        process_all_files_in_time_period(
            files=files_to_process[time_period],
            time_period=time_period,
            variable=variable,
            output_dir=output_dir,
            lon_min=lon_min,
            lon_max=lon_max,
            lat_min=lat_min,
            lat_max=lat_max,
            overwrite=overwrite,
            replace_original_files=replace_original_files,
        )

    print("\nDone.")
