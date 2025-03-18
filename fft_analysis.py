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

#velocity, temperature, salinity, mixed layer depth
mld = xr.open_dataset('copernicus-data/mld_day.nc')
flow = xr.open_dataset('copernicus-data/vel_day.nc')
temp = xr.open_dataset('copernicus-data/temp_day.nc')
salt = xr.open_dataset('copernicus-data/salinity_day.nc')


#chlorophyll, nitrate, oxygen
chlor = xr.open_dataset('copernicus-data/chlor_day.nc')
no3 = xr.open_dataset('copernicus-data/no3_day.nc')
o2 = xr.open_dataset('copernicus-data/o2_day.nc')


#Spatial means of data for now to show underlying temporal trends
def spatial_mean(dataset):
    dataset = np.asarray(dataset)
    dataset[np.isnan(dataset)] = 0
    spatmean =  np.mean(np.mean(dataset, axis = -1),axis = -1)
    return spatmean

#Phys
uo_spatmean = spatial_mean(flow.uo[:,0])
mld_spatmean = spatial_mean(mld.mlotst)
temp_spatmean = spatial_mean(temp.thetao[:,0])
salt_spatmean = spatial_mean(salt.so[:,0])

#bio
chlor_spatmean = spatial_mean(chlor.chl[:,0])
no3_spatmean = spatial_mean(no3.no3[:,0])
o2_spatmean = spatial_mean(o2.o2[:,0])



#TODO: this is very rough and ideally would be chunked into spatial areas before averaging in space. 

#FFT 
def four(data, dt):
    fft_ = fft(data)
    freq =fftfreq(len(data), dt)    
    fft_ = fft_[:len(data)//2]
    freq = freq[:len(data)//2]
    return fft_, freq

uo_fft, freq = four(uo_spatmean, 1)
mld_fft, freq = four(mld_spatmean,1)
temp_fft, freq = four(temp_spatmean, 1)
salt_fft, freq = four(salt_spatmean, 1)

chlor_fft, freq = four(chlor_spatmean, 1)
no3_fft, freq = four(no3_spatmean, 1)
o2_fft, freq = four(o2_spatmean, 1)

#Plotting FFTs
# Create four polar Axes and access them through the returned array
fig, axs = plt.subplots(4, 1, figsize = (6,15), sharex = True)
axs[0].plot(freq*360, np.abs(uo_fft))
axs[1].plot(freq*360, np.abs(mld_fft))
axs[2].plot(freq*360, np.abs(temp_fft))
axs[3].plot(freq*360, np.abs(salt_fft))

axs[0].set_title('uo')
axs[1].set_title('mixed layer depth')
axs[2].set_title('temperature')
axs[3].set_title('salinity')
axs[3].set_xlim([0,5])
axs[3].set_xlabel('frequency (1/year)')

plt.savefig('fft_phys.png')



#Plotting FFTs
# Create four polar Axes and access them through the returned array
fig, axs = plt.subplots(3, 1, figsize = (6,15), sharex = True)
axs[0].plot(freq*360, np.abs(chlor_fft))
axs[1].plot(freq*360, np.abs(no3_fft))
axs[2].plot(freq*360, np.abs(o2_fft))

axs[0].set_title('chlorophyll')
axs[1].set_title('no3')
axs[2].set_title('o2')
axs[2].set_xlim([0,5])
axs[2].set_xlabel('frequency (1/year)')


plt.savefig('fft_biog.png')