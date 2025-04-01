import xarray as xr
import numpy as np

#Import physical data files
uvel = xr.open_dataset('../copernicus-data/uvel2.nc')
# vvel = xr.open_dataset('../copernicus-data/vvel2.nc')
temp = xr.open_dataset('../copernicus-data/temp2.nc')
salt = xr.open_dataset('../copernicus-data/salinity2.nc')
botT = xr.open_dataset('../copernicus-data/botT2.nc')
mld = xr.open_dataset('../copernicus-data/mld2.nc')
ssh = xr.open_dataset('../copernicus-data/ssh2.nc')

#Import chemical data files
chl = xr.open_dataset('../copernicus-data/chl2.nc')
o2 = xr.open_dataset('../copernicus-data/o22.nc')
no3 = xr.open_dataset('../copernicus-data/no32.nc')
po4 = xr.open_dataset('../copernicus-data/po42.nc')
phyc = xr.open_dataset('../copernicus-data/phyc2.nc')
pp = xr.open_dataset('../copernicus-data/pp2.nc')
spco2 = xr.open_dataset('../copernicus-data/spco22.nc')
ph = xr.open_dataset('../copernicus-data/ph2.nc')

# #Drop depth in all files
# uvel = uvel.drop_vars("depth")
# vvel = vvel.drop_vars("depth")
# temp = temp.drop_vars("depth")
# salt = salt.drop_vars("depth")
# chl = chl.drop_vars("depth")
# o2 = o2.drop_vars("depth")
# no3 = no3.drop_vars("depth")
# po4 = po4.drop_vars("depth")
# phyc = phyc.drop_vars("depth")
# pp = pp.drop_vars("depth")
# ph = ph.drop_vars("depth")

#force velocity to have the same lat and lon as tempearture
#temperature matches the rest of the files
# uvel['latitude'] = temp['latitude']
# uvel['longitude'] = temp['longitude']



#Create land mask
land_mask = np.isnan(chl['chl'][0,:,:])
land_mask = np.where(land_mask, 0, 1)
land = xr.DataArray(land_mask[0], dims=["latitude", "longitude"], coords={"latitude": chl.latitude, "longitude": chl.longitude})
land = land.rename("land_mask")
land.to_netcdf('../copernicus-data/land_mask.nc')




#Update nc
uvel.to_netcdf('../copernicus-data/uvel_update.nc')
temp.to_netcdf('../copernicus-data/temp_update.nc')
salt.to_netcdf('../copernicus-data/salinity_update.nc')
botT.to_netcdf('../copernicus-data/botT_update.nc')
mld.to_netcdf('../copernicus-data/mld_update.nc')
ssh.to_netcdf('../copernicus-data/ssh_update.nc')
chl.to_netcdf('../copernicus-data/chl_update.nc')
o2.to_netcdf('../copernicus-data/o2_update.nc')
no3.to_netcdf('../copernicus-data/no3_update.nc')
po4.to_netcdf('../copernicus-data/po4_update.nc')
phyc.to_netcdf('../copernicus-data/phyc_update.nc')
pp.to_netcdf('../copernicus-data/pp_update.nc')
spco2.to_netcdf('../copernicus-data/spco2_update.nc')
ph.to_netcdf('../copernicus-data/ph_update.nc')    


# Resample bathymetry to match chlorophyll dataset resolution
bathy = xr.open_dataset('../gebco/GEBCO_26_Mar_2025_ab0e873a5abc/gebco_2024_n65.0346_s40.0333_w-19.9444_e13.0552.nc')
bathy = bathy.where(bathy.elevation <= 0, 0)  # Set land elevation to 0

# Define target grid based on chlorophyll dataset
target_lat = chl.latitude
target_lon = chl.longitude

# Use xarray's coarsen method to aggregate bathymetry data
lat_factor = len(bathy.lat) // len(target_lat)
lon_factor = len(bathy.lon) // len(target_lon)

bathy_coarse = bathy.coarsen(lat=lat_factor, lon=lon_factor, boundary="pad")
bathy_mean = bathy_coarse.mean().elevation.rename("mean_bath")
bathy_max = bathy_coarse.max().elevation.rename("min_depth")
bathy_min = bathy_coarse.min().elevation.rename("max_depth")
bathy_std = bathy_coarse.std().elevation.rename("std_bath")

# Align the coarse bathymetry with the chlorophyll dataset grid
bathy_mean = bathy_mean.interp(lat=target_lat, lon=target_lon)
bathy_max = bathy_max.interp(lat=target_lat, lon=target_lon)
bathy_min = bathy_min.interp(lat=target_lat, lon=target_lon)
bathy_std = bathy_std.interp(lat=target_lat, lon=target_lon)


bathy_mean = bathy_mean.drop_vars(["lat", "lon"])
bathy_max = bathy_max.drop_vars(["lat", "lon"])
bathy_min = bathy_min.drop_vars(["lat", "lon"])
bathy_std = bathy_std.drop_vars(["lat", "lon"])


# Save the results
bathy_mean.to_netcdf('../copernicus-data/bathy_mean.nc')
bathy_max.to_netcdf('../copernicus-data/bathy_max.nc')
bathy_min.to_netcdf('../copernicus-data/bathy_min.nc')
bathy_std.to_netcdf('../copernicus-data/bathy_std.nc')



