#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 11:16:55 2025

@author: maurerl
"""

import copernicusmarine
#Using subset API to download some data for grappling with

#dataset: string
#var_list: strings in list
#min_lon: number
#max_lon: number
#min_lat: number
#max_lat: number
#start: string date
#end: string date
#min_depth: number (unit in meters)
#max_depth: number (unit in meters)
#filename: string (.nc)

def data_collect(dataset, var_list, min_lon, max_lon, min_lat, max_lat, start, end, min_depth, max_depth, filename):
    copernicusmarine.subset(
            dataset_id=dataset,
            variables=var_list,
            minimum_longitude=min_lon,
            maximum_longitude=max_lon,
            minimum_latitude=min_lat,
            maximum_latitude=max_lat,
            start_datetime=start,
            end_datetime=end,
            minimum_depth=min_depth,
            maximum_depth=max_depth,
            output_filename = filename,
            output_directory = "copernicus-data")    



data_collect("cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m", ["mass_concentration_of_chlorophyll_a_in_sea_water"], -19.89, 13, 40.07, 65, "2023-01-01T00:00:00", "2024-01-01T00:00:00", 0, 5000, "chlr_a_month.nc" )

# Bottom temperature
#copernicusmarine.subset(
#  dataset_id="cmems_mod_nws_phy-bottomt_my_7km-2D_PT1H-i",
#  variables=["sea_water_potential_temperature_at_sea_floor"],
#  minimum_longitude=-19.89,
#  maximum_longitude=13,
#  minimum_latitude=40.07,
#  maximum_latitude=65,
#  start_datetime="2022-01-01T00:00:00",
#  end_datetime="2022-01-01T23:59:59",
#  minimum_depth=0,
 # maximum_depth=24,
#  output_filename = "bottomT_hr.nc",
 # output_directory = "copernicus-data"
#)
