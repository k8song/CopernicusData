import xarray as xr
import numpy as np


land = xr.open_dataset('../copernicus-data/land_mask.nc')
land_zoom = land.sel(latitude=slice(50, 53), longitude=slice(-6, -4))

#pcolor mesh so we think about the resolution more
plt.figure()
plt.pcolormesh(land_zoom.longitude, land_zoom.latitude, land_zoom.land_mask, shading='auto')
plt.plot(-5.05, 51.7, 'x', color="red")

uvel = xr.open_dataset('../copernicus-data/uvel_update.nc')
uvel_zoom = uvel.sel(latitude=slice(50, 53), longitude=slice(-6, -4))


plt.figure()
plt.pcolormesh(uvel_zoom.longitude, uvel_zoom.latitude,  uvel_zoom.uo[0,0])
plt.plot(-5.05, 51.7, 'x', color="red")


ssh = xr.open_dataset('../copernicus-data/ssh_update.nc')
ssh_zoom = ssh.sel(latitude=slice(50, 53), longitude=slice(-6, -4))
plt.figure()
plt.pcolormesh(ssh_zoom.longitude, ssh_zoom.latitude,  ssh_zoom.zos[0])
plt.plot(-5.05, 51.7, 'x', color="red")



"""
  I am thinking about boundary conditions/effects for the oceanic variables near the coastlines.
  Coastal vs non-coastal species as well?
  Ideally not in an estuary? 
  """