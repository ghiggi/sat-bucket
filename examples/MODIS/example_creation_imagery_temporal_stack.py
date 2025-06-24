import gpm # noqa
import matplotlib.pyplot as plt
import xarray as xr
import satbucket
from satbucket.analysis import split_by_overpass, overpass_to_dataset

# Define bucket directory
bucket_dir = "/home/ghiggi/data/MODIS_Bucket"

# Define area of interest
extent = [-87, -80, 60, 65] # [lon_min, lon_max, lat_min, lat_max]

# Read data over area of interest
df = satbucket.read(
    bucket_dir=bucket_dir,
   # columns=["water_vapor_infrared", "lon","lat", "time"],
    use_pyarrow=False,  # use rust parquet reader
    extent=extent,
    parallel="auto", # "row_groups", "columns"
    backend="polars" # "pandas"  
)

###------------------------------------------------------------------------.
#### Group data by satellite overpass 
n_minimum_pixels = 1200
n_overpass = 200
df = df.sort("time")
list_df_overpass = split_by_overpass(df, interval=None, max_overpass=n_overpass)
list_df_overpass = [d for d in list_df_overpass if len(d) > n_minimum_pixels]
list_ds_overpass = [overpass_to_dataset(df_overpass, x_dim="x", y_dim="y", x_index="x", y_index="y") for df_overpass in list_df_overpass]

###------------------------------------------------------------------------.
#### Plot satellite overpass
for ds_swath in list_ds_overpass[0:2]: 
    p = ds_swath["water_vapor_infrared"].gpm.plot_map(add_swath_lines=False)
    # p.axes.set_extent(extent)
    plt.show()

###------------------------------------------------------------------------.
#### Create temporal stack 
# Define grid where to remap data 
ds_aoi = satbucket.LonLatPartitioning(size=0.02, extent=extent).dataset_grid

# Check remmaping overpass onto common grid 
# ds_swath_remapped = ds_swath[["water_vapor_infrared"]].gpm.remap_on(ds_aoi)    

# Create stack of overpass 
list_ds_overpass = [ds_swath.assign_coords({"time": ds_swath["time"].values[0]}) for ds_swath in list_ds_overpass]   
list_ds_stack = [ds_swath.chunk().gpm.remap_on(ds_aoi) for ds_swath in list_ds_overpass]

ds_stack = xr.concat(list_ds_stack, dim="time")
 
###------------------------------------------------------------------------.
### Display temporal evolution
# - Optionally forward fill
da = ds_stack["water_vapor_infrared"].ffill(dim="time").compute()

# Display stack overpass 
for i in range(da["time"].size): 
    da_overpass = da.isel(time=i)
    p = da_overpass.gpm.plot_map()
    p.axes.set_extent(extent)
    plt.show() 
    
###------------------------------------------------------------------------.






 