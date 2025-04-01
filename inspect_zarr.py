import xarray as xr
import zarr
import json
import os
import numpy as np
import fsspec
import zarr
import json
import os
import numpy as np
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# #TODO: compare these two methods, open_dataset doesnt detect chunks as xr_openzarr, but it is used by earthmover https://earthmover.io/blog/xarray-open-zarr-improvements
# ds = xr.open_zarr(store=local_storage, consolidated=False, zarr_format=3) #NOTE: consolidated=False, True in zarr3 not supported
# # ds = xr.open_dataset(local_storage, engine="zarr", consolidated=False) # NOTE: as done by earthmover
storage_path = 'foundationmodel-v1/input/cm/test_sub/test/test_sub_v4.zarr'

# Get S3 credentials
key = os.getenv("S3_KEY")
secret = os.getenv("S3_SECRET")
endpoint = os.getenv("S3_ENDPOINT")

# Create a Zarr store that writes to a given bucket/path in Spaces.
fs = fsspec.filesystem(
  "s3",
  key=key,
  secret=secret,
  asynchronous=True,
  client_kwargs={"endpoint_url": endpoint}
  # client_kwargs={"endpoint_url": "https://foundationmodel-v1.tor1.digitaloceanspaces.com"}, #NOTE: endpoint more complete because it needs write access
)
#
s3_store = zarr.storage.FsspecStore(
  fs=fs,
  read_only=True,
  path=storage_path
)

ds = xr.open_zarr(store=s3_store, consolidated=False, zarr_format=3) #NOTE: consolidated=False, True in zarr3 not supported
print("Dataset info:")
print(f"Dimensions: {ds.dims}")  # Shows dimension names and sizes
print(f"Variables: {ds.data_vars}")  # Shows variable names
print(f"Coordinates: {ds.coords}")  # Shows coordinate names
print(f"Attributes: {ds.attrs}")  # Shows global dataset metadata #NOTE: metadata (consolidated metadata) not supported in zarr3
print(f"Chunks: {ds.chunks}")  # Shows chunk sizes for each variable
print(f"Data Variables: {ds.data_vars}")  # Shows data variables


# print("--------------------")
print("Zarr group info:")
# zgroup = zarr.open(store=local_storage, mode="r")
zgroup = zarr.open(store=s3_store, mode="r")
print(zgroup)
print(zgroup.tree())  # Shows Zarr group hierarchy
print(zgroup.info)  # General Zarr group info
print(json.dumps(dict(zgroup.attrs), indent=4))  # a #NOTE metadata (consolidated metadata) not supported in zarr3
print(zgroup["data"].chunks)  # Check if chunks exist

# # #
# # -------------------- Chunk Calculations --------------------
# print("--------------------")
# print("Chunk Calculations:")
# data_array = zgroup["data"]
# total_shape = data_array.shape  # (variable, time, latitude, longitude)
# chunk_shape = data_array.chunks
# num_chunks_per_dim = [np.ceil(tot / ch).astype(int) for tot, ch in zip(total_shape, chunk_shape)]
# total_chunks = np.prod(num_chunks_per_dim)

# # Compute chunk size in MB
# chunk_size_bytes = np.prod(chunk_shape) * np.dtype(data_array.dtype).itemsize
# chunk_size_mb = chunk_size_bytes / (1024 * 1024)

# # Print results
# print(f"Total Shape: {total_shape}")
# print(f"Chunk Shape: {chunk_shape}")
# print(f"Number of Chunks per Dimension: {num_chunks_per_dim}")  
# print(f"Total Chunks: {total_chunks}")
# print(f"Chunk Size: {chunk_size_mb:.2f} MB")

