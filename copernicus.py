#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 11:16:55 2025

@author: maurerl
"""

import copernicusmarine as cm
import os 
from dotenv import main


main.load_dotenv()  # take environment variables from .env.
cm.login(username = os.getenv("COPERNICUS_USERNAME"), password = os.getenv("COPERNICUS_PASSWORD"))



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
    cm.subset(
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



def data_collect2(dataset, var_list, min_lon, max_lon, min_lat, max_lat, filename):
    cm.subset(
            dataset_id=dataset,
            variables=var_list,
            minimum_longitude=min_lon,
            maximum_longitude=max_lon,
            minimum_latitude=min_lat,
            maximum_latitude=max_lat,
            output_filename = filename,
            output_directory = "copernicus-data")    

