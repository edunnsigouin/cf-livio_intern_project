#!/usr/bin/env python3

import os
import xarray as xr

url = "https://osdf-director.osg-htc.org/ncar/gdex/d651056/CESM2-LE/atm/proc/tseries/day_1/PRECT/b.e21.BHISTsmbb.f09_g17.LE2-1231.019.cam.h1.PRECT.18500101-18591231.nc"

outdir = "/nird/datapeak/NS9873K/etdu/raw/smile/test"
filename = url.split("/")[-1]

fullfile = f"{outdir}/{filename}"
subsetfile = f"{outdir}/scandinavia_{filename}"

os.makedirs(outdir, exist_ok=True)

# download using wget
print("Downloading file...")
os.system(f"wget -c -P {outdir} {url}")

# open dataset
print("Opening dataset...")
ds = xr.open_dataset(fullfile)

# subset Scandinavia
print("Subsetting region...")
ds_subset = ds.sel(
    lon=slice(2, 32.5),
    lat=slice(53, 73.5)
)

# save subset
print("Saving subset...")
ds_subset.to_netcdf(subsetfile)

ds.close()

# remove original file
print("Removing original file...")
os.remove(fullfile)

print("Done.")
print("Saved:", subsetfile)
