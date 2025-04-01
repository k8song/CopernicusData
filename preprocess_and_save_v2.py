import xarray as xr
import fsspec
import os
import time
import json
from dask.distributed import Client, LocalCluster
import sys
import os
from datetime import datetime
import json
import os
import fsspec
import zarr
import xarray as xr
from dotenv import load_dotenv
import numpy as np 
import asyncio

# ---------------------------- DASK CLUSTER SETUP ----------------------------

def setup_dask_cluster(n_workers=3, threads_per_worker=3, memory_limit="5GB"):
    #TODO: optimize the number of workers and threads per worker and memory limit
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        dashboard_address=":8787"
    )
    client = Client(cluster)

    print(f"✅ Dask Dashboard: {client.dashboard_link}")
    return client


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Fix velocity ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def velocity_fix(vel_path, comp_path, years, storage, key, secret, endpoint):

#open like in the load_data_for_years function. 
    fs = fsspec.filesystem("s3", anon=True, client_kwargs={"endpoint_url": "https://s3.waw3-1.cloudferro.com"})
    dataset_files = []

#opening velocity 
    for year in years:
        pattern = f"{vel_path}/{year}/*.nc"  
        files = fs.glob(pattern)
        dataset_files.extend([fs.open(file) for file in files])
    if not dataset_files:
        raise FileNotFoundError(f"No dataset files found for years: {years}")

    print(f"✅ Found {len(dataset_files)} velocity files for {years}. Loading datasets...")

#open with _mfdataset since we may potentially have separate netcdfs for different years 
    ds_vel = xr.open_mfdataset(
        dataset_files,
        engine="h5netcdf",
        combine="by_coords",
        coords="minimal",
        parallel=True,
        chunks={},)

#reset dataset_files so that this does the same thing for the comparative variable 
    dataset_files = []
#TODO: Unsure about this step 
    year_ = 2020 #arbitrary year because we don't need much data for this
    pattern = f"{comp_path}/{year_}/*.nc"  
    files = fs.glob(pattern)
    dataset_files.extend([fs.open(file) for file in files])
    if not dataset_files:
        raise FileNotFoundError(f"No dataset files found for years: {years}")
        
    print(f"✅ Found {len(dataset_files)} comparison file(s) for {year_}. Loading datasets...")
    
    #open with _mfdataset since we may potentially have separate netcdfs for different years 
    ds_comp = xr.open_mfdataset(
        dataset_files,
        engine="h5netcdf",
        combine="by_coords",
        coords="minimal",
        parallel=True,
        chunks={},) 
    
    #redo the coordinates
    ds_vel['latitude'] = ds_comp['latitude']
    ds_vel['longitude'] = ds_comp['longitude']
    
    return ds_vel
    


#use fixed velocity to create land mask
def create_land(ds_vel):
      # Create land mask
    land_mask = np.isnan(ds_vel['uo'][0, :, :])
    land_mask = land_mask.rename('land_mask')

    # Use xarray's masking instead of np.where
    land = land_mask.astype(np.float32).fillna(1).where(land_mask, 0)

    land = land.expand_dims(time=ds_vel.time)
    # Convert to Dataset
    ds_land = land.to_dataset(name='land_mask')
 
    chunks = {"time": 12, "latitude": 32, "longitude": 32}
    ds_land = ds_land.chunk(chunks)

    return ds_land    

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Bring in the bathymetry ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def bathymetry_fix(bathymetry_path, ds_time):
    """
    After bringing in the data, we need to add a time dimension in xarray. 
    Bathymetry will not change in time.
    """
    #from local files, bring in bathymetry (bathymetry_path = ~/oceanos/copernicus-data)
    #TODO: make this neater
    bathy_mean = xr.open_dataset(f"{bathymetry_path}/bathy_mean.nc")
    bathy_max = xr.open_dataset(f"{bathymetry_path}/bathy_max.nc")
    bathy_min = xr.open_dataset(f"{bathymetry_path}/bathy_min.nc")  
    bathy_std = xr.open_dataset(f"{bathymetry_path}/bathy_std.nc")


    # add time dimension to the bathymetry datasets
    # take time from ds_time = ds_vel
    bathy_mean = bathy_mean.expand_dims(time = ds_time.time)
    bathy_max = bathy_max.expand_dims(time = ds_time.time)
    bathy_min = bathy_min.expand_dims(time = ds_time.time)
    bathy_std = bathy_std.expand_dims(time = ds_time.time)

    #merge the bathymetry datasets
    bathy_mean, bathy_max, bathy_min, bathy_std = xr.align(bathy_mean, bathy_max, bathy_min, bathy_std, join="override")
    ds_bathymetry = xr.merge([bathy_mean, bathy_max, bathy_min, bathy_std]) #makes merged dataset 
    
    #think I'm setting chunks like in other datasets
    chunks = {}
    ds_bathymetry = ds_bathymetry.chunk(chunks)
    ds_bathymetry = ds_bathymetry.astype(np.float32)

    return ds_bathymetry

# ---------------------------- DATA LOADING -----------------------------------
    
#do not include the velocity path in your base paths, as this is explicity called later for the fix. 

def load_data_for_years(base_paths, years, storage, key, secret, endpoint):
    """
    Lazily loads data from S3 for the specified years using xarray.
    """
    fs = fsspec.filesystem("s3", anon=True, client_kwargs={"endpoint_url": "https://s3.waw3-1.cloudferro.com"})
    dataset_files = []

#want to leave out velocity since that's already done? 
    for base_path in base_paths:
        for year in years:
            pattern = f"{base_path}/{year}/*.nc"  
            files = fs.glob(pattern)
            dataset_files.extend([fs.open(file) for file in files])

    if not dataset_files:
        raise FileNotFoundError(f"No dataset files found for years: {years}")

    print(f"✅ Found {len(dataset_files)} files for {years}. Loading datasets...")


    ds_ = xr.open_mfdataset(
        dataset_files,
        engine="h5netcdf",
        combine="by_coords",
        coords="minimal",
        parallel=True,
        chunks={},
    )
    
        #paths for velocity and for comparison variable. This could be improved. 
    vel_path = "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_PHY_004_009/cmems_mod_nws_phy-uv_my_7km-3D_P1M-m_202012"
    comp_path = "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_PHY_004_009/cmems_mod_nws_phy-t_my_7km-3D_P1M-m_202012"
      
    #Fix velocity and output as ds_vel,   
    ds_vel = velocity_fix(vel_path, comp_path, years, storage, key=key, secret=secret, endpoint=endpoint)
    print("✅ Velocity fixed.")

    #make land mask from fixed velocity
    ds_land = create_land(ds_vel)
    print("✅ Land mask created.")    

    #make bathymetry from fixed velocity and local files 
    ds_bathymetry = bathymetry_fix("../copernicus-data", ds_vel['uo'])
    print("✅ Bathymetry included.")

    # ds = xr.merge([ds_, ds_vel, ds_bathymetry, ds_land])  # Merge the problem datasets 
    # ds = xr.merge([ds_vel, ds_land])  # test merges 

    # Drop depth dimension if present (select first layer)
    if "depth" in ds.dims:
        ds = ds.isel(depth=0).drop_vars("depth", errors="ignore")
    
    print("✅ Data loaded and combined.")

     #Deleted variable mapping 

    return ds

# ------------------------- ZARR v3 SAVING -------------------------

def save_to_zarr_v3(ds, storage_path, chunks, key, secret, endpoint):
    """
    Saves dataset to Zarr v3 format, storing it in an S3 bucket.
    """
    # for DigitalOcean Spaces, delete the anon=True and add key and secret from AWS

    # Create a Zarr store that writes to a given bucket/path in Spaces.
    fs = fsspec.filesystem(
        "s3",
        key=key,
        secret=secret,
        client_kwargs={"endpoint_url": "https://foundationmodel-v1.tor1.digitaloceanspaces.com"}, #NOTE: endpoint more complete because it needs write access
        #client_kwargs={"endpoint_url": endpoint},
        asynchronous=True # Enable async mode for working with Dask or other async libraries
    )

    s3_store = zarr.storage.FsspecStore(
        path=storage_path,
        fs=fs,
    )

    print(f"📦 Storing Zarr dataset at {storage_path}")


    ds = ds.chunk(chunks=chunks) # set chunk size

    ds.to_zarr(
        store=s3_store,
        mode="w", #overwrite existing file if there is one
        zarr_format=3,
        consolidated=False, # NOTE metadata not supported in zarr v3 so consolidated metadata is not supported
    )

    print("✅ Zarr file saved successfully.")






#------------------------- MAIN FUNCTION 2-------------------------
def main(years="ALL"):
    """
    Loads dataset, processes it, and stores each variable as a separate Zarr v3 file in S3.
    """

    # Load environment variables from .env file
    load_dotenv()

    # Get S3 credentials
    key = os.getenv("S3_KEY")
    secret = os.getenv("S3_SECRET")
    endpoint = os.getenv("S3_ENDPOINT")

    # Define base paths for data
    base_paths = [
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_PHY_004_009/cmems_mod_nws_phy-t_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_PHY_004_009/cmems_mod_nws_phy-s_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_PHY_004_009/cmems_mod_nws_phy-bottomt_my_7km-2D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_PHY_004_009/cmems_mod_nws_phy-mld_my_7km-2D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_PHY_004_009/cmems_mod_nws_phy-ssh_my_7km-2D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-kd_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-chl_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-o2_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-no3_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-po4_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-phyc_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-pp_my_7km-3D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-spco2_my_7km-2D_P1M-m_202012",
        "s3://mdl-native-13/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-ph_my_7km-3D_P1M-m_202012",
    ]
    


    # Determine year range and storage path
    if years == "ALL":
        year_range = list(range(1993, 2025))
        storage_path = "input/cm/all_years/test/"
        storage_path_suffix = f"all_years.zarr"
    else:
        year_range = [years]
        storage_path = "input/cm/test_sub/test/"
        storage_path_suffix = "test_sub_1yr.zarr"

    zarr_path = f"{storage_path}{storage_path_suffix}"

    # Setup Dask cluster
    client = setup_dask_cluster()

    try:
        # Define chunking strategy
        chunks = {"time": 12, "latitude": 32, "longitude": 32}

        # Load dataset
        ds = load_data_for_years(base_paths, year_range, storage_path, key=key, secret=secret, endpoint=endpoint)
        save_to_zarr_v3(ds, zarr_path, chunks, key=key, secret=secret, endpoint=endpoint)

    finally:
        client.close()
        print("✅ Dask client closed.")




if __name__ == "__main__":
    main(years=2020)  
    # main(years="ALL")

