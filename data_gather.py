#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 12 17:00:07 2025

@author: maurerl
"""


from copernicus import *
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Physical data (7)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Current
# data_collect("cmems_mod_nws_phy-uv_my_7km-3D_P1M-m", ["eastward_sea_water_velocity"], -19.8, 13, 41, 65.1, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "uvel2.nc" )
# data_collect("cmems_mod_nws_phy-uv_my_7km-3D_P1M-m", ["northward_sea_water_velocity"],-19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "vvel2.nc" )  
data_collect("cmems_mod_nws_phy-t_my_7km-3D_P1M-m", ["sea_water_potential_temperature"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "temp2.nc")
data_collect("cmems_mod_nws_phy-s_my_7km-3D_P1M-m", ["sea_water_salinity"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "salinity2.nc")
data_collect("cmems_mod_nws_phy-bottomt_my_7km-2D_P1M-m", ["sea_water_potential_temperature_at_sea_floor"],-19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "botT2.nc")
data_collect("cmems_mod_nws_phy-mld_my_7km-2D_P1M-m", ["ocean_mixed_layer_thickness_defined_by_sigma_theta"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "mld2.nc")
data_collect("cmems_mod_nws_phy-ssh_my_7km-2D_P1M-m", ["sea_surface_height_above_geoid"],-19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "ssh2.nc")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Biological data (9)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
data_collect("cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m", ["mass_concentration_of_chlorophyll_a_in_sea_water"], -19.88889, 12.99967, 40.06667, 65.00125,"2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "chl2.nc")
data_collect("cmems_mod_nws_bgc-o2_my_7km-3D_P1M-m", ["mole_concentration_of_dissolved_molecular_oxygen_in_sea_water"],-19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "o22.nc")
data_collect("cmems_mod_nws_bgc-no3_my_7km-3D_P1M-m", ["mole_concentration_of_nitrate_in_sea_water"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "no32.nc")
data_collect("cmems_mod_nws_bgc-po4_my_7km-3D_P1M-m", ["mole_concentration_of_phosphate_in_sea_water"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "po42.nc")
data_collect("cmems_mod_nws_bgc-phyc_my_7km-3D_P1M-m", ["mole_concentration_of_phytoplankton_expressed_as_carbon_in_sea_water"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "phyc2.nc")
data_collect("cmems_mod_nws_bgc-pp_my_7km-3D_P1M-m", ["net_primary_production_of_biomass_expressed_as_carbon_per_unit_volume_in_sea_water"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "pp2.nc")
data_collect("cmems_mod_nws_bgc-spco2_my_7km-2D_P1M-m", ["surface_partial_pressure_of_carbon_dioxide_in_sea_water"], -19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "spco22.nc")
data_collect("cmems_mod_nws_bgc-ph_my_7km-3D_P1M-m", ["sea_water_ph_reported_on_total_scale"],-19.88889, 12.99967, 40.06667, 65.00125, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "ph2.nc")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#bathymetry data  (4)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#bathymetry is fine resolution GPPCO 
#MIN, MAX, MIN, STD