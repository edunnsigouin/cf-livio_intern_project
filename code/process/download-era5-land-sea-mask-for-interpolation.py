"""
Download ERA5 land-sea mask at 0.25° resolution for Scandinavia.
"""

import os
import cdsapi

from livio_intern_project import config


path_out = config.dirs["raw_era5_daily"]

output_file = os.path.join(
    path_out,
    "era5_land_sea_mask_0.25x0.25_scandinavia.nc",
)


if __name__ == "__main__":

    os.makedirs(path_out, exist_ok=True)

    client = cdsapi.Client()

    client.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable": "land_sea_mask",
            "year": "2020",
            "month": "01",
            "day": "01",
            "time": "00:00",
            "grid": [0.25, 0.25],

            # Area format is:
            # [north, west, south, east]
            #
            # Your coordinates:
            # latitude: 73.5 to 53.0
            # longitude: 2.0 to 32.5
            "area": [73.5, 2.0, 53.0, 32.5],

            "format": "netcdf",
        },
        output_file,
    )

    print(f"Saved land-sea mask to:\n{output_file}")
