#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 17:00:07 2025

@author: maurerl
"""

from copernicus import *
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Physical data
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Daily currents, only need surface 
data_collect("cmems_mod_nws_phy-uv_my_7km-3D_P1D-m", ["northward_sea_water_velocity", "eastward_sea_water_velocity"], -19.89, 13, 40.07, 65, "2015-01-01T00:00:00", "2020-01-01T00:00:00", 0, 1, "vel_day.nc" )
#Temperature
data_collect("cmems_mod_nws_phy-t_my_7km-3D_P1D-m", ["sea_water_potential_temperature"], -19.89, 13, 40.07, 65, "2015-01-01T00:00:00", "2020-01-01T00:00:00", 0, 1, "temp_day.nc" )
#mixed layer depth: this seems to drop out at time 2023-04-18? 
data_collect("cmems_mod_nws_phy-mld_my_7km-2D_P1D-m", ["ocean_mixed_layer_thickness_defined_by_sigma_theta"], -19.89, 13, 40.07, 65, "2015-01-01T00:00:00", "2020-01-01T00:00:00", 0, 1, "mld_day.nc" )
#salinity 
data_collect("cmems_mod_nws_phy-s_my_7km-3D_P1D-m", ["sea_water_salinity"], -19.89, 13, 40.07, 65, "2015-01-01T00:00:00", "2020-01-01T00:00:00", 0, 1, "salinity_day.nc" )


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Biological data
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Chlorophyll    
data_collect("cmems_mod_nws_bgc-chl_my_7km-3D_P1D-m", ["mass_concentration_of_chlorophyll_a_in_sea_water"], -19.89, 13, 40.07, 65, "2015-01-01T00:00:00", "2020-01-01T00:00:00", 0, 1, "chlor_day.nc" )
#no3
data_collect("cmems_mod_nws_bgc-no3_my_7km-3D_P1D-m", ["mole_concentration_of_nitrate_in_sea_water"], -19.89, 13, 40.07, 65, "2015-01-01T00:00:00", "2020-01-01T00:00:00", 0, 1, "no3_day.nc" )
#o2
data_collect("cmems_mod_nws_bgc-o2_my_7km-3D_P1D-m", ["mole_concentration_of_dissolved_molecular_oxygen_in_sea_water"], -19.89, 13, 40.07, 65, "2015-01-01T00:00:00", "2020-01-01T00:00:00", 0, 1, "o2_day.nc" )
