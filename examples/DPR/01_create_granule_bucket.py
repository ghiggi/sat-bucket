####----------------------------------------------------------------------.
#### Important notes
# - Set ulimit -n 999999 in the terminal before launching the script !
# - Configure GPM-API: https://gpm-api.readthedocs.io/en/latest/03_quickstart.html
# - Download GPM DPR data with GPM-API: https://gpm-api.readthedocs.io/en/latest/tutorials/tutorial_02_RADAR_2A.html

####----------------------------------------------------------------------.
import os 
import gpm 
import dask 
import logging
from gpm.io.local import get_local_filepaths
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
    #### Define satellite bucket product, variables and settings 
    
    # Define geographic grid bucket directory 
    bucket_dir = "/home/ghiggi/data/GPM_DPR_Bucket_Granules"
    os.makedirs(bucket_dir, exist_ok=True)
      
    # Define partitioning
    spatial_partitioning = LonLatPartitioning(size=[4, 4], labels_decimals=0)
     
    # Define processing options 
    parallel = True
    max_dask_total_tasks = 1000
 
    # Define GPM product
    product = "2A-DPR"
    product_type = "RS"
    version = 7   

    ####----------------------------------------------------------------------.
    #### List all available files 
    print("Listing available granules")
    filepaths = get_local_filepaths(product=product, 
                                    product_type=product_type, 
                                    version=version)
 
    # filepath = filepaths[0]

    ####----------------------------------------------------------------------.
    #### Define the granule filepath to dataframe conversion function 
    def create_dataframe_from_granule(filepath):
       #### Define the variables of interest
       # Define GPM variables relevant for footprints with precipitation
       variables = [
           "precipRateAve24",
           "precipRateESurface",
           "precipRateESurface2",
           "precipRateNearSurface",
           "precipWaterIntegrated", # precipWaterIntegrated_Liquid, precipWaterIntegrated_Solid during decoding
           
           "zFactorFinalESurface",
           "zFactorFinalNearSurface",
           
           "qualityFlag",
           "dataQuality",
           
           "phaseNearSurface",
           "typePrecip",
           "flagBB", 
           "flagShallowRain", 
           "flagHeavyIcePrecip", 
           "flagHail",
           "flagGraupelHail",
           "flagAnvil", 
           
           "heightStormTop", 
           "heightZeroDeg",
           "heightBB",
           "widthBB",
           
           "binClutterFreeBottom", 
           "binRealSurface",
           "binBBBottom",
           "binBBTop",
           
           "flagPrecip",      # 0 No precipitation, >1 precipitation
           
           # PIA
           "pathAtten",
           "piaFinal",
           "reliabFlag",
           
           "sunLocalTime", 
           "localZenithAngle",
           "landSurfaceType",
           "elevation",
           "flagSurfaceSnowfall",
           "seaIceConcentration",
           "snowIceCover",
           
           "flagSigmaZeroSaturation",
           "sigmaZeroCorrected",
           "sigmaZeroMeasured",
           "snRatioAtRealSurface",
           
           # "zFactorMeasured",
           "zFactorFinal",
           "precipRate",
           "paramDSD", 
           "airTemperature",
            
       ]
       
       #---------------------------------------------------------------------.
       #### Open the granules 
       scan_mode = "FS" 
       open_granule_kwargs = {
           "scan_mode": scan_mode,
           "groups": None,
           "variables": variables,
           "decode_cf": True,
           "chunks": -1,
       }
       
       ds = gpm.open_granule_dataset(filepath, **open_granule_kwargs)
       
       # Precompute the enture granules 
       # - Faster than compute after stacking because of the very small chunking of native HDF files
       ds = ds.compute()
      
       # Close connection 
       ds.close()
       
       #---------------------------------------------------------------------.
       #### Stack granules per beam and select only ones with precipitation 
       # - This allow to speed up computations as discard lot of data (~x25)
       # - By performing stacking, you restrict yourself to just use retrievals 
       #   that do not need the spatial horizontal neighbour data
           
       # Stack
       # - "beam" or "pixel" are cosidered spatial dimensions by GPM-API
       ds_beam = ds.stack(dim={"beam": ["cross_track", "along_track"]})
       idx_precip = ds_beam["flagPrecip"] > 0
       ds_beam = ds_beam.isel(beam=idx_precip)
      
       #---------------------------------------------------------------------.
       #### Compute custom variables / features           
       ds = ds_beam
       
       # Compute variables for both frequecies: Ku and Ka
       for band in ["Ku", "Ka"]:
           ds_f = ds.sel({"radar_frequency": band})
           ds[f"dataQuality_{band}"] = ds_f["dataQuality"]
           for var in ds.gpm.frequency_variables:
               # Standard variables
               ds[f"{var}_{band}"] = ds_f[var]     

           # Custom variables
           # - REFC takes 2-3 seconds each on full granule !
           # - EchoDepth takes 5-6 seconds each on full granule !
           ds[f"heightRealSurface_{band}"] = ds_f.gpm.get_height_at_bin(bins=ds_f["binRealSurface"])
           ds[f"REFC_{band}"] = ds_f.gpm.retrieve("REFC")
           ds[f"REFC_without_bb_{band}"] = ds_f.gpm.retrieve("REFC", mask_bright_band=True)  
           ds[f"REFC_liquid_{band}"] = ds_f.gpm.retrieve("REFC", mask_solid_phase=True) 
           ds[f"REFC_solid_{band}"] = ds_f.gpm.retrieve("REFC", mask_liquid_phase=True)     
           ds[f"REFC_solid_without_bb_{band}"] = ds_f.gpm.retrieve("REFC", mask_liquid_phase=True, mask_bright_band=True)
           ds[f"REFC_liquid_without_bb_{band}"] = ds_f.gpm.retrieve("REFC", mask_solid_phase=True, mask_bright_band=True)
           ds[f"REFCH_{band}"] = ds_f.gpm.retrieve("REFCH")
           ds[f"EchoDepth30dBZ_{band}"] = ds_f.gpm.retrieve("EchoDepth", threshold=30)
           ds[f"EchoDepth30dBZ_solid_phase_{band}"] = ds_f.gpm.retrieve("EchoDepth", threshold=30, mask_liquid_phase=True)
               
       # Compute additional variables
       ds["precipitationType"] = ds.gpm.retrieve("flagPrecipitationType", method="major_rain_type")   
       ds["heightClutterFreeBottom"] = ds.gpm.retrieve("heightClutterFreeBottom")
       
   
       ds["EchoDepth45dBZ"] = ds.gpm.retrieve("EchoDepth", threshold=45)
       ds["EchoDepth50dBZ"] = ds.gpm.retrieve("EchoDepth", threshold=50)
       
       ds["EchoDepth30dBZ_solid_phase"] = ds.gpm.retrieve("EchoDepth", threshold=30, mask_liquid_phase=True)
       ds["EchoDepth40dBZ_solid_phase"] = ds.gpm.retrieve("EchoDepth", threshold=40, mask_liquid_phase=True)
       ds["EchoDepth45dBZ_solid_phase"] = ds.gpm.retrieve("EchoDepth", threshold=45, mask_liquid_phase=True)
       ds["EchoDepth50dBZ_solid_phase"] = ds.gpm.retrieve("EchoDepth", threshold=50, mask_liquid_phase=True)
     
       ds["EchoTopHeight30dBZ"] = ds.gpm.retrieve("EchoTopHeight", threshold=30)
       ds["EchoTopHeight40dBZ"] = ds.gpm.retrieve("EchoTopHeight", threshold=40)
       ds["EchoTopHeight50dBZ"] = ds.gpm.retrieve("EchoTopHeight", threshold=50)
       ds["EchoTopHeight60dBZ"] = ds.gpm.retrieve("EchoTopHeight", threshold=60)
               
       # Take maximum temperature 
       ds["airTemperatureMax"] = ds["airTemperature"].max(dim="range")
       ds["precipRateMax"] = ds["precipRate"].max(dim="range")
       
       # Take DSD parameters statistics across column 
       ds["Nw_Max"] = ds["Nw"].max(dim="range")
       ds["Nw_Mean"] = ds["Nw"].median(dim="range")    # 2 secs 
       ds["Nw_Std"] = ds["Nw"].std(dim="range", skipna=True)
       ds["dBNw_Max"] = ds["dBNw"].max(dim="range")
       ds["dBNw_Mean"] = ds["dBNw"].median(dim="range") # 2 secs 
       ds["dBNw_Std"] = ds["dBNw"].std(dim="range", skipna=True)
       ds["Dm_Max"] = ds["Dm"].max(dim="range")
       ds["Dm_Mean"] = ds["Dm"].median(dim="range") # 2 secs 
       ds["Dm_Std"] = ds["Dm"].std(dim="range", skipna=True)
                       
       # Extract surface DSD variables
       surface_vars = ["Nw", "dBNw", "Dm"]
       ds_surface = ds[[*surface_vars]].gpm.slice_range_at_bin(
           ds["binClutterFreeBottom"])
       for var in surface_vars:
           ds[f"{var}NearSurface"] = ds_surface[var]
  
       #---------------------------------------------------------------------.
       # Compute statistics of solid and liquid phase 
       da_liquid_precip_rate = ds["precipRate"].where(ds["airTemperature"] > 273.15)
       da_solid_precip_rate = ds["precipRate"].where(ds["airTemperature"] < 273.15)
       ds["precipRateSolid_Max"] = da_solid_precip_rate.max(dim="range")
       ds["precipRateLiquid_Max"] = da_liquid_precip_rate.max(dim="range")
              
       #---------------------------------------------------------------------.
       # Add Hail Indicators
       ds["POH"] = ds.gpm.retrieve("POH") # Take 6 secs
       ds["MESHS"] = ds.gpm.retrieve("MESHS") # Take 6 secs
           
       # # Takes 8 seconds each
       ds["VIL"] = ds.gpm.retrieve("VIL")
      
       # # Takes 14 seconds each
       ds["VILD"] = ds.gpm.retrieve("VILD")
       ds["VILD_over_depth"] = ds.gpm.retrieve("VILD", use_echo_top=False)
       
       # # Takes 30 seconds each
       ds["SHI"] = ds.gpm.retrieve("SHI")
       ds["MESH"] = ds.gpm.retrieve("MESH")
       ds["POSH"] = ds.gpm.retrieve("POSH")
       
       #---------------------------------------------------------------------.
       #### Discard unrelevant variables  
       ds = ds.drop_vars(ds.gpm.frequency_variables)
       ds = ds.drop_vars(ds.gpm.vertical_variables)
       ds = ds.drop_vars([
           # Variables not used
           "binClutterFreeBottom", 
           "binBBBottom",
           "binBBTop",
           "typePrecip", # used for flagPrecipitationType
           # Remove multifrequency and 3D coordinates
           "dataQuality",
           "height"])
       
       
       # for var in ds.data_vars:
       #     print(var, ds[var].dims)
       
       #---------------------------------------------------------------------.
       #### Convert to pandas dataframe 
       df = ds.gpm.to_pandas_dataframe()
       
       #---------------------------------------------------------------------.
       #### Filter the dataframe 
       # Select footprints where is raining in the column 
       df = df.loc[df["flagPrecip"] > 0, :]
       
       #---------------------------------------------------------------------.
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


