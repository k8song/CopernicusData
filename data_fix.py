import xarray as xr
import numpy as np

#Import physical data files
vel = xr.open_dataset('copernicus-data/vel.nc')
temp = xr.open_dataset('copernicus-data/temp.nc')
salt = xr.open_dataset('copernicus-data/salinity.nc')
botT = xr.open_dataset('copernicus-data/botT.nc')
mld = xr.open_dataset('copernicus-data/mld.nc')
ssh = xr.open_dataset('copernicus-data/ssh.nc')

#Import chemical data files
chl = xr.open_dataset('copernicus-data/chl.nc')
o2 = xr.open_dataset('copernicus-data/o2.nc')
no3 = xr.open_dataset('copernicus-data/no3.nc')
po4 = xr.open_dataset('copernicus-data/po4.nc')
phyc = xr.open_dataset('copernicus-data/phyc.nc')
pp = xr.open_dataset('copernicus-data/pp.nc')
spco2 = xr.open_dataset('copernicus-data/spco2.nc')
ph = xr.open_dataset('copernicus-data/ph.nc')

#Drop depth in all files
vel = vel.drop_vars("depth")
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
vel['latitude'] = temp['latitude']
vel['longitude'] = temp['longitude']


#Create land mask
land_mask = np.isnan(temp['thetao'][0,:,:])
land_mask = land_mask.rename('land_mask')
land = np.where(land_mask == True, 1, 0)[0]
land = xr.DataArray(land, dims=('latitude', 'longitude'), coords={'latitude': temp['latitude'], 'longitude': temp['longitude']})
land.to_netcdf('copernicus-data/land_mask.nc')

#Update nc
vel.to_netcdf('copernicus-data/vel_update.nc')
temp.to_netcdf('copernicus-data/temp_update.nc')
salt.to_netcdf('copernicus-data/salinity_update.nc')
botT.to_netcdf('copernicus-data/botT_update.nc')
mld.to_netcdf('copernicus-data/mld_update.nc')
ssh.to_netcdf('copernicus-data/ssh_update.nc')
chl.to_netcdf('copernicus-data/chl_update.nc')
o2.to_netcdf('copernicus-data/o2_update.nc')
no3.to_netcdf('copernicus-data/no3_update.nc')
po4.to_netcdf('copernicus-data/po4_update.nc')
phyc.to_netcdf('copernicus-data/phyc_update.nc')
pp.to_netcdf('copernicus-data/pp_update.nc')
spco2.to_netcdf('copernicus-data/spco2_update.nc')
ph.to_netcdf('copernicus-data/ph_update.nc')    