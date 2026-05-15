"""
hard coded paths in cf-livio_intern_project
"""

cf_space_datalake       = "/nird/datalake/NS9873K/etdu/"
cf_space_datapeak       = "/nird/datapeak/NS9873K/etdu/"
proj                    = "/nird/home/edu061/cf-livio_intern_project/"
data                    = proj + "data/"
fig                     = proj + "fig/"

raw_cesm2_le            = cf_space_datalake + "raw/smile/cesm2_le/scandinavia/"
raw_gfdl_spear_med_le   = cf_space_datalake + "raw/smile/gfdl_spear_med_le/scandinavia/"
raw_era5_daily          = cf_space_datapeak + "raw/era5/continuous-format/daily/scandinavia/"
interpolated_era5_daily = cf_space_datalake + "raw/era5/scandinavia/"

dirs = {"proj":proj,
        "data":data,
        "fig":fig,
        "raw_cesm2_le":raw_cesm2_le,
        "raw_gfdl_spear_med_le":raw_gfdl_spear_med_le,
        "raw_era5_daily":raw_era5_daily,
        "interpolated_era5_daily":interpolated_era5_daily,
}        


