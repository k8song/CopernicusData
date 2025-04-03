import xarray as xr
import numpy as np

#Import physical data files
uvel = xr.open_dataset('../copernicus-data/uvel.nc')
vvel = xr.open_dataset('../copernicus-data/vvel.nc')
temp = xr.open_dataset('../copernicus-data/temp.nc')
salt = xr.open_dataset('../copernicus-data/salinity.nc')
botT = xr.open_dataset('../copernicus-data/botT.nc')
mld = xr.open_dataset('../copernicus-data/mld.nc')
ssh = xr.open_dataset('../copernicus-data/ssh.nc')

#Import chemical data files
chl = xr.open_dataset('../copernicus-data/chl.nc')
o2 = xr.open_dataset('../copernicus-data/o2.nc')
no3 = xr.open_dataset('../copernicus-data/no3.nc')
po4 = xr.open_dataset('../copernicus-data/po4.nc')
phyc = xr.open_dataset('../copernicus-data/phyc.nc')
pp = xr.open_dataset('../copernicus-data/pp.nc')
spco2 = xr.open_dataset('../copernicus-data/spco2.nc')
ph = xr.open_dataset('../copernicus-data/ph.nc')

#Drop depth in all files
uvel = uvel.drop_vars("depth")
vvel = vvel.drop_vars("depth")
temp = temp.drop_vars("depth")
salt = salt.drop_vars("depth")
chl = chl.drop_vars("depth")
o2 = o2.drop_vars("depth")
no3 = no3.drop_vars("depth")
po4 = po4.drop_vars("depth")
phyc = phyc.drop_vars("depth")
pp = pp.drop_vars("depth")
ph = ph.drop_vars("depth")

#force velocity to have the same lat and lon as tempearture
#temperature matches the rest of the files
uvel['latitude'] = temp['latitude']
uvel['longitude'] = temp['longitude']

vvel['longitude'] = temp['longitude']
vvel['latitude'] = temp['latitude']


#Create land mask
land_mask = np.isnan(temp['thetao'][0,:,:])
land_mask = land_mask.rename('land_mask')
land = np.where(land_mask == True, 1, 0)[0]
land = xr.DataArray(land, dims=('latitude', 'longitude'), coords={'latitude': temp['latitude'], 'longitude': temp['longitude']} )
land = land.expand_dims(time = temp.time)
land.to_netcdf('../copernicus-data/land_mask.nc')

#Update nc
uvel.to_netcdf('../copernicus-data/uvel_update.nc')
vvel.to_netcdf('../copernicus-data/vvel_update.nc')
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


#upload gebco bathymetry netcdf
bathy = xr.open_dataset('../gebco/GEBCO_26_Mar_2025_ab0e873a5abc/gebco_2024_n65.0346_s40.0333_w-19.9444_e13.0552.nc')
# Resample the bathy dataset to match the coarser resolution of the chlr dataset. This doesnt match the coordinates right though
#mean
#disregarding the edges, lon_factor = 26, lat_factor = 16
blat = np.asarray(bathy.lat)
blon = np.asarray(bathy.lon)
clat = np.asarray(chl.latitude)
clon = np.asarray(chl.longitude)

# 2D course via mean
bathy_mean = np.empty((clat.shape[0],clon.shape[0]))
bathy_max = np.zeros_like(bathy_mean)
bathy_min = np.zeros_like(bathy_max)
bathy_std = np.zeros_like(bathy_max)

bathy = np.asarray(bathy.elevation)
bathy =  np.where(bathy > 0, 0, bathy) #get rid of land


#Create a coarse grid version of the bathymetry
#Versions are: mean, max, min, stdev

#For each cell in the chl dataset, find the corresponding cells in the bathy dataset and do operation
for i in range(1,len(clon)):
     for j in range(1,len(clat)):
         
         #creates boxes of coordinates to use for the bathymetry
         lonmin = np.argwhere((blon > clon[i-1])).min()
         lonmax = np.argwhere((blon <= clon[i])).max()
         latmin = np.argwhere((blat > clat[j-1])).min()
         latmax = np.argwhere((blat <= clat[j])).max()
         
         #mean, max, min, std
         bathy_mean[j,i] = np.mean(bathy[latmin:latmax, lonmin:lonmax])
         bathy_max[j,i] =  np.max(bathy[latmin:latmax, lonmin:lonmax])
         bathy_min[j,i] =  np.min(bathy[latmin:latmax, lonmin:lonmax])
         bathy_std[j,i] =  np.std(bathy[latmin:latmax, lonmin:lonmax])
         
         
#Fill in the edges
lonmax = np.argwhere((blon <= clon[0])).max()
latmax = np.argwhere((blat <= clat[0])).max()
bathy_mean[0,:] = np.mean(bathy[:latmax, :lonmax])
bathy_max[0,:] = np.max(bathy[:latmax, :lonmax])
bathy_min[0,:] = np.min(bathy[:latmax, :lonmax])
bathy_std[0,:] = np.std(bathy[:latmax, :lonmax])



#Create xarray dataset
bathy_mean = xr.DataArray(bathy_mean, dims=('latitude', 'longitude'), coords={'latitude': chl['latitude'], 'longitude': chl['longitude']}, name = "mean bathymetry" )
bathy_max = xr.DataArray(bathy_max, dims=('latitude', 'longitude'), coords={'latitude': chl['latitude'], 'longitude': chl['longitude']}, name = "max bathymetry" )
bathy_min = xr.DataArray(bathy_min, dims=('latitude', 'longitude'), coords={'latitude': chl['latitude'], 'longitude': chl['longitude']}, name = "min bathymetry" )
bathy_std = xr.DataArray(bathy_std, dims=('latitude', 'longitude'), coords={'latitude': chl['latitude'], 'longitude': chl['longitude']} , name = "std bathymetry")


#Save
bathy_mean.to_netcdf('../copernicus-data/bathy_mean.nc')
bathy_max.to_netcdf('../copernicus-data/bathy_max.nc')
bathy_min.to_netcdf('../copernicus-data/bathy_min.nc')
bathy_std.to_netcdf('../copernicus-data/bathy_std.nc')


