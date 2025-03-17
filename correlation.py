#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 17 15:53:15 2025

@author: maurerl
"""

import numpy as np 
import xarray as xr
import matplotlib.pyplot as plt
from scipy.fft import fft, fftfreq
from scipy import stats 

#velocity, temperature, salinity, mixed layer depth in one .nc file
mld = xr.open_dataset('copernicus-data/mld_day.nc')
flow = xr.open_dataset('copernicus-data/vel_day.nc')
temp = xr.open_dataset('copernicus-data/temp_day.nc')
salt = xr.open_dataset('copernicus-data/salinity_day.nc')


#Spatial means of data for now to show underlying temporal trends
def spatial_mean(dataset):
    spatmean =  np.mean(np.mean(dataset, axis = -1),axis = -1)
    spatmean = np.asarray(spatmean)[:,0]
    return spatmean

uo_spatmean = spatial_mean(flow.uo)
vo_spatmean = spatial_mean(flow.vo)
temp_spatmean = spatial_mean(temp.thetao)
salt_spatmean = spatial_mean(salt.so)

#TODO: this is very rough and ideally would be chunked into spatial areas before averaging in space. 

#FFT 
def four(data, dt):
    fft_ = fft(data)
    freq =fftfreq(len(data), dt)
    
    fft_ = fft_[:len(data)//2]
    freq = freq[:len(data)//2]
    return fft_, freq

uo_fft, freq = four(uo_spatmean, 1)
vo_fft, freq = four(vo_spatmean,1)
temp_fft, freq = four(temp_spatmean, 1)
salt_fft, freq = four(salt_spatmean, 1)

#Plotting FFTs
# Create four polar Axes and access them through the returned array
fig, axs = plt.subplots(4, 1, figsize = (6,15), sharex = True)
axs[0].plot(freq*360, np.abs(uo_fft))
axs[1].plot(freq*360, np.abs(vo_fft))
axs[2].plot(freq*360, np.abs(temp_fft))
axs[3].plot(freq*360, np.abs(salt_fft))

axs[0].set_title('uo')
axs[1].set_title('vo')
axs[2].set_title('temperature')
axs[3].set_title('salinity')
axs[3].set_xlim([0,10])

axs[3].set_xlabel('frequency (1/year)')


