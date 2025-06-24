####----------------------------------------------------------------------.
#### Important notes
# - Set ulimit -n 999999 in the terminal before launching the script !
# - Install pyhdf with: conda install pyhdf
# - Install pyspectral with: conda install pyspectral
####----------------------------------------------------------------------.
import os 
import dask 
import glob
import logging
from satbucket import write_granules_bucket, LonLatPartitioning

# dask.config.set({'distributed.worker.multiprocessing-method': 'spawn'})
dask.config.set({'distributed.worker.multiprocessing-method': 'forkserver'})
dask.config.set({'distributed.worker.use-file-locking': 'False'})

from dask.distributed import Client, LocalCluster


if __name__ == "__main__": #  https://github.com/dask/distributed/issues/2520    
    ####----------------------------------------------------------------------.
    #### Define Dask Distributed Cluster   
    # Set environment variable to avoid HDF locking
    os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"
    
    # Set number of workers 
    # dask.delayed run n_workers*2 concurrent processes
    available_workers = int(os.cpu_count()/2)
    num_workers = dask.config.get("num_workers", available_workers)
        
    # Create dask.distributed local cluster
    # --> Use multiprocessing to avoid netCDF multithreading locks ! 
    cluster = LocalCluster(
        n_workers=num_workers,
        threads_per_worker=1, # important to set to 1 to avoid netcdf locking ! 
        processes=True,
        silence_logs=logging.WARN,
    )
    
    client = Client(cluster)
    # client.ncores()
    # client.nthreads()
    
    ####----------------------------------------------------------------------.
    #### Define GridBucket product, variables and settings 
    
    # Define geographic grid bucket directory 
    bucket_dir = "/home/ghiggi/data/MODIS_Granule_Bucket"
    os.makedirs(bucket_dir, exist_ok=True)
      
    # Define partitioning
    spatial_partitioning = LonLatPartitioning(size=[1, 1], labels_decimals=0)
     
    # Define processing options 
    parallel = True
    max_dask_total_tasks = 1000
 
    ####----------------------------------------------------------------------.
    #### List all available files 
    print("Listing available granules")
    filepaths = glob.glob("/home/ghiggi/Downloads/sat_bucket/data/MODIS/MOD05*")
 
    # filepath = filepaths[0]

    ####----------------------------------------------------------------------.
    #### Define the granule filepath to dataframe conversion function 
    def create_dataframe_from_granule(filepath):
        from satpy import Scene
        import xarray as xr
        scn = Scene(reader='modis_l2', filenames=[filepath])
        scn.available_dataset_names()
        
        # Read data at 1 km resolution filepaths[0]
        scn.load(["water_vapor_infrared", "scan_start_time"])
        time_unit = scn["scan_start_time"].attrs["units"]
       
        # Remap the data 
        remapped_scn = scn.resample(resampler='native')
       
        # Get xarray Dataset
        ds = remapped_scn.to_xarray().compute()
       
        # Add time coordinate 
        ds["scan_start_time"].attrs["unit"] = time_unit
        ds["scan_start_time"] = xr.decode_cf(ds[["scan_start_time"]], decode_times=True)["scan_start_time"]
        
        # Specify satellite scan id in {granule_id/orbit_number}-{along_track_id} format 
        # --> Granule ID or Orbit Number is not provided for MODIS L2 products. We skip this step 
        # --> Creation of overpass stack around where orbit starts and ends will be inaccurate
        
        # Ensure lon/lat/time coordinates are present !
        ds = ds.rename_vars({"longitude": "lon", "latitude": "lat", "scan_start_time": "time"})
       
        # Reshape to dataframe
        ds = ds.stack(dim={"pixel": ["x", "y"]})
        df = ds.to_dataframe()     
        return df 

    ####----------------------------------------------------------------------.
    #### Compute Granule Buckets
    # ---> Multiprocessing for high performance
    # ---> It process by batches of 2*n_workers
    writer_kwargs = {}
    write_granules_bucket(
        # Bucket Input/Output configuration
        filepaths=filepaths,
        bucket_dir=bucket_dir,
        spatial_partitioning=spatial_partitioning,
        granule_to_df_func=create_dataframe_from_granule,
        # Processing options
        parallel=True,
        max_concurrent_tasks=None,
        max_dask_total_tasks=max_dask_total_tasks,
        # Writer kwargs 
        **writer_kwargs,
    )
    ####----------------------------------------------------------------------.


