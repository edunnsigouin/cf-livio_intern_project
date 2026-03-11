"""
Collection of useful miscellaneous functions
"""

import time
import numpy  as np
import xarray as xr
from scipy    import signal
import os
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

def tic():
    """
    matlab style tic function
    """
    global startTime_for_tictoc
    startTime_for_tictoc = time.time()
    return

def toc():
    """
    matlab style toc function
    """                        
    if 'startTime_for_tictoc' in globals():
        print("Elapsed time is " + str(time.time() - startTime_for_tictoc) + " seconds.")
    else:
        print("Toc: start time not set")
    return


def get_dim(grid,time_flag):
    """   
    imports data dimensions given a grid 
    """
    if grid == '0.25x0.25':
        from forsikring import dim_025x025 as dim
    elif grid == '0.5x0.5':
        from forsikring import dim_05x05 as dim
    elif grid == '1.0x1.0':
        from forsikring import dim_1x1 as dim
    elif grid == '0.25x0.25_0.5x0.5':
        from forsikring import dim_025x025_05x05 as dim
        
    if time_flag == 'weekly':
        dim.time  = dim.timescale
        dim.ntime = dim.ntimescale

    return dim


def xy_mean(ds):
    """ 
    calculates xy mean over dims lat and lon
    with cosine weighting in lat. Input is xarray
    dataarray or dataset
    """
    weights = np.cos(np.deg2rad(ds.latitude))
    ds      = ds.weighted(weights).mean(dim=('latitude','longitude'))
    return ds        

def rm_lpyr_days(data):
    """ 
    removes leap-year days from daily xrray dataset
    """
    return data.sel(time=~((data.time.dt.month == 2) & (data.time.dt.day == 29)))

def is_leap_year(year):
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        return True
    else:
        return False

def get_season(ds,season):
    """
    Extracts times belonging to a given season
    input = xarray dataset or dataarray
    """
    months = ds['time.month']
    if season == 'ndjfm': index = (months >= 11) | (months <= 3)
    elif season == 'mjjas': index = (months >= 5) & (months <= 9)
    elif season == 'annual': index = (months >= 1) & (months <= 12)
    elif season == 'djf': index = (months >= 12) | (months <= 2)
    elif season == 'mam': index = (months >= 3) & (months <= 5)
    elif season == 'jja': index = (months >= 6) & (months <= 8)
    elif season == 'son': index = (months >= 9) & (months <= 11)
    elif season == 'jfm': index = (months >= 1) & (months <= 3)
    return ds.sel(time=index)


def subselect_xy_domain_from_dim(dim,domain,grid):
    """  
    sub-selects xy domain from dim                                                                                                       
    """
    if grid == '0.25x0.25':
        if domain == 'scandinavia':
            dim.latitude   = np.flip(np.arange(53,73.25,0.25))
            dim.longitude  = np.arange(2,32.25,0.25)
        elif domain == 'vestland':
            dim.latitude   = np.flip(np.arange(59,62.75,0.25))
            dim.longitude  = np.arange(4,8.75,0.25)
        elif domain == 'southern_norway':
            dim.latitude   = np.flip(np.arange(57,66.25,0.25))
            dim.longitude  = np.arange(3.5,12.75,0.25)
        elif domain == 'bergen':
            dim.latitude   = np.flip(np.arange(60.25,61,0.25))
            dim.longitude  = np.arange(5,5.75,0.25)
        elif domain == 'oslo':
            dim.latitude   = np.flip(np.arange(59.5,60.25,0.25))
            dim.longitude  = np.arange(10.25,11,0.25)            
        elif domain == 'northern_europe':
            dim.latitude   = np.flip(np.arange(53.25,73.75,0.25))
            dim.longitude  = np.arange(-27,45.25,0.25)
        elif domain == 'southern_europe':
            dim.latitude   = np.flip(np.arange(33,53.25,0.25))
            dim.longitude  = np.arange(-27,45.25,0.25)
        elif domain == 'western_europe':
            dim.latitude   = np.arange(33,73.75,0.25)
            dim.longitude  = np.arange(-27,9.0,0.25)
        elif domain == 'eastern_europe':
            dim.latitude   = np.arange(33,73.75,0.25)
            dim.longitude  = np.arange(9.0,45.25,0.25)            
        elif domain == 'iberia':
            dim.latitude   = np.flip(np.arange(35,45.25,0.25))
            dim.longitude  = np.arange(-12,3.25,0.25)
        elif domain == 'europe2':
            dim.latitude   = np.flip(np.arange(43.5,63.75,0.25))
            dim.longitude  = np.arange(-9,27.25,0.25)
        elif domain == 'europe3':
            dim.latitude   = np.flip(np.arange(48.5,58.75,0.25))
            dim.longitude  = np.arange(0,18.25,0.25)
            
    elif grid == '0.5x0.5':
        if domain == 'scandinavia':
            dim.latitude   = np.flip(np.arange(53,73.5,0.5))
            dim.longitude  = np.arange(2,32.5,0.5)
        elif domain == 'southern_norway':
            dim.latitude   = np.flip(np.arange(57,66.5,0.5))
            dim.longitude  = np.arange(3.5,13,0.5)
        elif domain == 'vestland':
            dim.latitude   = np.flip(np.arange(59,63,0.5))
            dim.longitude  = np.arange(4,9,0.5)
        elif domain == 'northern_europe':
            dim.latitude   = np.flip(np.arange(53,74,0.5))
            dim.longitude  = np.arange(-27,45.5,0.5)
        elif domain == 'southern_europe':
           dim.latitude   = np.flip(np.arange(33,53,0.5))
           dim.longitude  = np.arange(-27,45.5,0.5)
        elif domain == 'iberia':
            dim.latitude   = np.flip(np.arange(35,45.5,0.5))
            dim.longitude  = np.arange(-12,3.5,0.5)
        elif domain == 'europe2':
            dim.latitude   = np.flip(np.arange(43.5,64,0.5))
            dim.longitude  = np.arange(-9,27.5,0.5)
        elif domain == 'europe3':
            dim.latitude   = np.flip(np.arange(48.5,59,0.5))
            dim.longitude  = np.arange(0,18.5,0.5)
            
    dim.nlatitude  = dim.latitude.size
    dim.nlongitude = dim.longitude.size
    return dim


def to_netcdf_with_packing_and_compression(data, filename, dtype='int16', zlib=True, complevel=5):
    """
    Writes an xarray DataArray or Dataset to a NetCDF file, applying packing and zlib compression.
    
    Parameters:
    - data (xarray.DataArray or xarray.Dataset): The data to write to file.
    - filename (str): The path to the output NetCDF file.
    - dtype (str): The target dtype for packing. Default is 'int16'.
    - zlib (bool): Whether to apply zlib compression. Default is True.
    - complevel (int): Compression level from 1 to 9. Default is 5.
    """
    
    def calculate_scale_and_offset(min_val, max_val, dtype):
        """
        Calculate scale factor and add offset for packing data.
        Adds a buffer to avoid the minimum value being set to the fill value.
        """
        data_range = max_val - min_val
        int_min, int_max = np.iinfo(dtype).min, np.iinfo(dtype).max

        # Add a buffer to min_val to prevent it from becoming a fill value due to rounding
        buffer = data_range / (int_max - int_min)
        add_offset = min_val - buffer
        scale_factor = (max_val - add_offset) / (int_max - 1)  # Use int_max - 1 to ensure max value is representable

        return scale_factor, add_offset

    encoding = {}
    fill_value = np.iinfo(np.dtype(dtype)).min  # Use minimum representable value as fill value
    
    data_vars = list(data.data_vars) if isinstance(data, xr.Dataset) else [data.name]
    
    for var in data_vars:
        da = data[var] if isinstance(data, xr.Dataset) else data
        
        min_val = float(da.min())
        max_val = float(da.max())
        
        scale_factor, add_offset = calculate_scale_and_offset(min_val, max_val, dtype)
        
        encoding[var] = {
            'dtype': dtype,
            'scale_factor': scale_factor,
            'add_offset': add_offset,
            'zlib': zlib,
            '_FillValue': fill_value,
            'complevel': complevel
        }
    
    # Write the data to a NetCDF file with the specified encoding
    data.to_netcdf(filename, encoding=encoding)

    return




def to_netcdf_with_compression(data,comp_lev,path,filename):
    """
    Uses xarray's native compression to write to netcdf with compression
    using to_netcdf function
    """
    # Define your compression options
    #compression_opts = {'zlib': True, 'complevel': comp_lev, 'shuffle': True}
    compression_opts = {'zlib': True, 'complevel': comp_lev}
    
    # Check if data is a DataArray or Dataset, set encoding and write to netcdf
    if isinstance(data, xr.DataArray):
        encoding = {data.name: compression_opts}  # Use the name of the DataArray
        data.to_netcdf(path+filename, format='NETCDF4', engine='netcdf4', encoding=encoding)
    elif isinstance(data, xr.Dataset):
        encoding = {var: compression_opts for var in data.data_vars}  # Apply to all variables
        data.to_netcdf(path+filename, format='NETCDF4', engine='netcdf4', encoding=encoding)
    else:
        raise TypeError("The array must be either an xarray DataArray or Dataset")
    return


def compress_file(comp_lev,ncfiletype,filename,path_out):
    """  
    wrapper for compressing file using nccopy
    """
    cmd           = 'nccopy -k ' + str(ncfiletype) + ' -s -d ' + str(comp_lev) + ' '
    filename_comp = 'temp_' + filename
    os.system(cmd + path_out + filename + ' ' + path_out + filename_comp)
    os.system('mv ' + path_out + filename_comp + ' ' + path_out + filename)
    return


def create_custom_colormap_with_white_center(orig_cmap, levels):
    """
    Create a custom colormap that modifies an original colormap. If there is an even number of colors,
    the middle two colors are set to white. If there is an odd number of colors, the middle color is set to white.

    Parameters:
    orig_cmap (matplotlib.colors.Colormap): Original colormap to modify.
    levels (list of float): Levels at which the colors change.

    Returns:
    matplotlib.colors.Colormap: Customized colormap with white at the center.
    """
    n_colors = len(levels) - 1
    new_colors = plt.cm.get_cmap(orig_cmap)(np.linspace(0, 1, n_colors))

    if n_colors % 2 == 0:
        # Even number of colors; set the middle two colors to white
        middle_indices = [n_colors // 2 - 1, n_colors // 2]
        new_colors[middle_indices] = [1, 1, 1, 1]  # RGBA for white
    else:
        # Odd number of colors; set the middle color to white
        middle_index = n_colors // 2
        new_colors[middle_index] = [1, 1, 1, 1]  # RGBA for white

    return mcolors.ListedColormap(new_colors)
