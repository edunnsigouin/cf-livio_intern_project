"""
Collection of useful miscellaneous functions
"""

import time
import numpy  as np
import xarray as xr
from scipy    import signal
import os

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


