#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 17:00:07 2025

@author: maurerl
"""

#from copernicus import *
import xarray as xr
import tempfile
import shutil
import glob 
from copernicus import *
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Test data collect function (PASS)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Daily currents, only need surface 
#data_collect("cmems_mod_nws_phy-uv_my_7km-3D_P1D-m", ["northward_sea_water_velocity", "eastward_sea_water_velocity"], -19.89, 13, 40.07, 65, "2021-01-01T00:00:00", "2024-01-01T00:00:00", 0, 1, "vel_day.nc" )


#Temperature
#data_collect("cmems_mod_nws_phy-t_my_7km-3D_P1D-m", ["sea_water_potential_temperature"], -19.89, 13, 40.07, 65, "2021-01-01T00:00:00", "2024-01-01T00:00:00", 0, 1, "temp_day.nc" )

#mixed layer depth
data_collect("cmems_mod_nws_phy-mld_my_7km-2D_P1D-m", ["ocean_mixed_layer_thickness_defined_by_sigma_theta"], -19.89, 13, 40.07, 65, "2021-01-01T00:00:00", "2024-01-01T00:00:00", 0, 1, "mld_day.nc" )

#salinity 
data_collect("cmems_mod_nws_phy-s_my_7km-3D_P1D-m", ["sea_water_salinity"], -19.89, 13, 40.07, 65, "2021-01-01T00:00:00", "2024-01-01T00:00:00", 0, 1, "salinity_day.nc" )



#current data 
#3D dataset of monthyl uo and vo. 
#data_collect("cmems_mod_nws_phy-uv_my_7km-3D_P1M-m", ["northward_sea_water_velocity", "eastward_sea_water_velocity"],  -19.89, 13, 40.07, 65, "2023-01-01T00:00:00", "2024-01-01T00:00:00", 0, 5000, "current_month.nc"  )

