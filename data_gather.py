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
data_collect("cmems_mod_nws_phy-uv_my_7km-3D_P1M-m", ["eastward_sea_water_velocity"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "uvel.nc" )
data_collect("cmems_mod_nws_phy-uv_my_7km-3D_P1M-m", ["northward_sea_water_velocity"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "vvel.nc" )  
data_collect("cmems_mod_nws_phy-t_my_7km-3D_P1M-m", ["sea_water_potential_temperature"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "temp.nc")
data_collect("cmems_mod_nws_phy-s_my_7km-3D_P1M-m", ["sea_water_salinity"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "salinity.nc")
data_collect("cmems_mod_nws_phy-bottomt_my_7km-2D_P1M-m", ["sea_water_potential_temperature_at_sea_floor"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "botT.nc")
data_collect("cmems_mod_nws_phy-mld_my_7km-2D_P1M-m", ["ocean_mixed_layer_thickness_defined_by_sigma_theta"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "mld.nc")
data_collect("cmems_mod_nws_phy-ssh_my_7km-2D_P1M-m", ["sea_surface_height_above_geoid"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "ssh.nc")

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Biological data (9)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
data_collect("cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m", ["mass_concentration_of_chlorophyll_a_in_sea_water"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "chl.nc")
data_collect("cmems_mod_nws_bgc-o2_my_7km-3D_P1M-m", ["mole_concentration_of_dissolved_molecular_oxygen_in_sea_water"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "o2.nc")
data_collect("cmems_mod_nws_bgc-no3_my_7km-3D_P1M-m", ["mole_concentration_of_nitrate_in_sea_water"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "no3.nc")
data_collect("cmems_mod_nws_bgc-po4_my_7km-3D_P1M-m", ["mole_concentration_of_phosphate_in_sea_water"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "po4.nc")
data_collect("cmems_mod_nws_bgc-phyc_my_7km-3D_P1M-m", ["mole_concentration_of_phytoplankton_expressed_as_carbon_in_sea_water"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "phyc.nc")
data_collect("cmems_mod_nws_bgc-pp_my_7km-3D_P1M-m", ["net_primary_production_of_biomass_expressed_as_carbon_per_unit_volume_in_sea_water"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "pp.nc")
data_collect("cmems_mod_nws_bgc-spco2_my_7km-2D_P1M-m", ["surface_partial_pressure_of_carbon_dioxide_in_sea_water"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "spco2.nc")
data_collect("cmems_mod_nws_bgc-ph_my_7km-3D_P1M-m", ["sea_water_ph_reported_on_total_scale"], -19.89, 13, 40.07, 65, "2020-01-01T00:00:00", "2021-01-01T00:00:00", 0, 1, "ph.nc")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#bathymetry data  (4)
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#bathymetry is fine resolution GPPCO 
#MIN, MAX, MIN, STD