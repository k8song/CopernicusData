import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from shapely.geometry import Polygon
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.collections import PatchCollection





land = xr.open_dataset('../copernicus-data/land_mask.nc')

land_zoom = land.sel(latitude=slice(51.6, 51.8), longitude=slice(-5.2, -4.75))
#This is 3 latitudes and 4 longitudes

indices = np.where(land_zoom.land_mask == 1)

# Extract the corresponding latitude and longitude values
latitudes = land_zoom.latitude.values[indices[0]]
longitudes = land_zoom.longitude.values[indices[1]]

# Combine latitudes and longitudes into a list of coordinates
coordinates = list(zip(latitudes, longitudes))
polygon = Polygon(coordinates)  #polyhon of coordinates



# Create a figure with a geographic projection
plt.figure(figsize=(8, 6))
ax = plt.axes(projection=ccrs.PlateCarree())  # Use PlateCarree for lat/lon data

# Plot the land mask
plt.pcolormesh(
    land_zoom.longitude, land_zoom.latitude, land_zoom.land_mask,
    shading='auto', transform=ccrs.PlateCarree()
)

# Add the 'x' marker
plt.plot(
    -5.04, 51.7, 'x', markersize=10, markeredgewidth=4, color="red",
    transform=ccrs.PlateCarree()
)



# Add a map of the UK (coastlines and borders)
ax.coastlines(resolution='10m', color='magenta', linewidth=1)
ax.add_feature(cfeature.BORDERS, linestyle=':', edgecolor='magenta')

# Add gridlines for reference
ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')


# Set the extent to match the extent of land_zoom
ax.set_extent([-5.2, -4.75,
               51.6, 51.8], crs=ccrs.PlateCarree())



land_zoom = land.sel(latitude=slice(51.6, 51.8), longitude=slice(-5.2, -4.75))
#This is 3 latitudes and 4 longitudes


# Set the title and show the plot
plt.title('Land Mask with Polygon')
plt.show()
