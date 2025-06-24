####----------------------------------------------------------------------.
#### Important notes
# - Set ulimit -n 999999 in the terminal before launching the script !
# - Install pygac with: pip install pygac
# - Donwload TLE data: 
#   1. Create a TLE directory  
#   2. Inside TLE directory do: wget -r -np -nH --cut-dirs=5 -R "index.html*" https://public.cmsaf.dwd.de/data/perm/pygac-fdr/test_data/tle/
#   --> Alternatively download manually from https://celestrak.org/NORAD/archives/request.php
# - Specify the TLE directory in reader_kwargs below
 
####----------------------------------------------------------------------.
import os 
import numpy as np 
import dask 
import glob
import logging
from satbucket import write_granules_bucket, LonLatPartitioning
from satbucket.info import get_key_from_filepath

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
    bucket_dir = "/home/ghiggi/data/AVHRR_Granules_Bucket"
    os.makedirs(bucket_dir)
      
    # Define partitioning
    spatial_partitioning = LonLatPartitioning(size=[1, 1], labels_decimals=0)
     
    # Define processing options 
    parallel = True
    max_dask_total_tasks = 1000
 
        
    ####----------------------------------------------------------------------.
    #### List all available files 
    print("Listing available granules")
    filepaths = glob.glob("/home/ghiggi/Downloads/sat_bucket/data/AVHRR/BRN.HRPT.*")
 
    # filepath = filepaths[0]
 
    ####----------------------------------------------------------------------.
    #### Define the granule filepath to dataframe conversion function 
    def create_dataframe_from_granule(filepath):
        #----------------------------------------------------------------------.
        from satpy import Scene

        # Define TLE settings    
        reader_kwargs={
            'tle_dir': '/home/ghiggi/data/TLE',
            'tle_name': 'TLE_%(satname)s.txt',
        }
        
        # Read data using satpy
        scn = Scene(reader='avhrr_l1b_gaclac', 
                    filenames=[filepath],
                    reader_kwargs=reader_kwargs)
        scn.available_dataset_names()
        
        # Load channels and auxiliary info
        channels = ['1', '2', '3', '4', '5']
        ancillary = [
            'solar_zenith_angle',
            'sensor_zenith_angle',
            'solar_azimuth_angle',
            'sensor_azimuth_angle',
            'sun_sensor_azimuth_difference_angle',
           #  'qual_flags',
        ]
        scn.load(channels+ancillary)
        
        # Remap the data to single resolution
        remapped_scn = scn.resample(resampler='native')
        
        # Get xarray Dataset
        ds = remapped_scn.to_xarray().compute()
        
        # Specify satellite scan id in {granule_id/orbit_number}-{along_track_id} format 
        filename_pattern = "{creation_site:3s}.{transfer_mode:4s}.{platform_id:2s}.D{start_time:%y%j.S%H%M}.E{end_time:%H%M}.B{orbit_number:05d}{end_orbit_last_digits:02d}.{station:2s}"
        granule_id = get_key_from_filepath(filepath, key="orbit_number", filename_pattern=filename_pattern)      
        ds["scan_id"] = ds["x"].astype(str)
        ds["scan_id"].data = np.char.add(str(granule_id) + "-", ds["x"].data.astype(str))
        
        # Ensure lon/lat/time coordinates are present
        ds = ds.rename_vars({"longitude": "lon", "latitude": "lat", "acq_time": "time"})
       
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


