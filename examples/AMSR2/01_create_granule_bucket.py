####----------------------------------------------------------------------.
#### Important notes
# - Set ulimit -n 999999 in the terminal before launching the script !
# - Configure GPM-API: https://gpm-api.readthedocs.io/en/latest/03_quickstart.html
# - Download 1C-AMSR2-GCOMW1 product with GPM-API:
#   --> https://gpm-api.readthedocs.io/en/latest/tutorials/tutorial_02_PMW_1C.html

####----------------------------------------------------------------------.
import logging
import os

import dask
import gpm
import numpy as np
import pandas as pd
import xarray as xr
from gpm.io.local import get_local_filepaths
from gpm.utils.geospatial import extend_extent
from gpm.utils.orbit import get_orbit_mode
from gpm.utils.slices import get_slice_size

from satbucket import LonLatPartitioning, write_granules_bucket

# Do not warn for invalid coordinates
gpm.config.set({"warn_invalid_geolocation": False})
gpm.config.set({"warn_non_contiguous_scans": False})

# Dask configurations
# dask.config.set({'distributed.worker.multiprocessing-method': 'spawn'})
dask.config.set({"distributed.worker.multiprocessing-method": "forkserver"})
dask.config.set({"distributed.worker.use-file-locking": "False"})

from dask.distributed import Client, LocalCluster  # noqa

if __name__ == "__main__":  #  https://github.com/dask/distributed/issues/2520
    ####----------------------------------------------------------------------.
    #### Define Dask Distributed Cluster
    # Set environment variable to avoid HDF locking
    os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"

    # Set number of workers
    # dask.delayed run n_workers*2 concurrent processes
    available_workers = int(os.cpu_count() / 2)
    num_workers = dask.config.get("num_workers", available_workers)

    # Create dask.distributed local cluster
    # --> Use multiprocessing to avoid netCDF multithreading locks !
    cluster = LocalCluster(
        n_workers=num_workers,
        threads_per_worker=1,  # important to set to 1 to avoid netcdf locking !
        processes=True,
        silence_logs=logging.WARN,
    )

    client = Client(cluster)
    # client.ncores()
    # client.nthreads()

    ####----------------------------------------------------------------------.
    #### Define satellite bucket product, variables and settings

    # Define geographic grid bucket directory
    bucket_dir = "/home/ghiggi/data/AMSR2_Bucket_Granules"
    os.makedirs(bucket_dir, exist_ok=True)

    # Define partitioning
    spatial_partitioning = LonLatPartitioning(size=[2, 2], labels_decimals=0)

    # Define processing options
    parallel = True
    max_dask_total_tasks = 1000

    # Define GPM product
    product = "1C-AMSR2-GCOMW1"
    product_type = "RS"
    version = 7

    ####----------------------------------------------------------------------.
    #### List all available files
    print("Listing available granules")
    filepaths = get_local_filepaths(product=product, product_type=product_type, version=version)

    # filepath = filepaths[0]

    ####----------------------------------------------------------------------.
    #### Define the granule filepath to dataframe conversion function
    def create_dataframe_from_granule(filepath):
        # ---------------------------------------------------------------------.
        #### Define the variables of interest
        variables = [
            [
                "Quality",
                #'FractionalGranuleNumber',
                # 'SCaltitude',
                "SClatitude",  # --> to determine orbit_mode
                # 'SClongitude',
                "Tc",
                "incidenceAngle",
                "sunGlintAngle",
                "sunLocalTime",
            ],
        ]

        # Define area of interest
        geographic_extent = [120, 145, -76, -64]

        # ---------------------------------------------------------------------.
        #### Open the granules
        dt = gpm.open_granule_datatree(
            filepath=filepath,
            variables=variables,
            chunks=-1,
            cache=False,
        )

        # Remove scan modes not needed
        dt = dt.drop_nodes("S6")

        # Identify orbit slices over AOI !
        try:
            scan_modes = ["S1", "S2", "S3", "S4", "S5"]
            dict_isel_dicts = {
                scan_mode: dt[scan_mode]
                .to_dataset()
                .gpm.get_crop_slices_by_extent(extend_extent(geographic_extent, padding=2))
                for scan_mode in scan_modes
            }
        except Exception:
            return None  # No intersecting data

        # Check same size on each scan mode
        n_slices_per_scan_mode = [len(v) for v in dict_isel_dicts.values()]
        n_slices = np.unique(n_slices_per_scan_mode)
        if len(n_slices) > 1:
            raise ValueError("Different cropping area across scan modes")

        # Check slice size of at least size 5
        dict_isel_dicts = {
            scan_mode: [isel_dict for isel_dict in list_isel_dicts if get_slice_size(isel_dict["along_track"]) > 5]
            for scan_mode, list_isel_dicts in dict_isel_dicts.items()
        }
        n_slices_per_scan_mode = [len(v) for v in dict_isel_dicts.values()]
        n_slices = np.unique(n_slices_per_scan_mode)
        if len(n_slices) > 1:
            raise ValueError("In a single scan mode, a slice of size < 5 has been dropped.")

        n_slices = n_slices[0]

        # Create dataframe over each orbit slice
        list_df = []

        for i in range(n_slices):

            # Crop scan modes over region of interest
            dict_ds = {}
            for scan_mode in scan_modes:
                # - Subset regions
                ds = dt[scan_mode].to_dataset().isel(dict_isel_dicts[scan_mode][i])
                # - Constant variables across scan mode
                ds["incidenceAngle"] = ds["incidenceAngle"].isel(pmw_frequency=0)
                ds["sunGlintAngle"] = ds["sunGlintAngle"].isel(pmw_frequency=0)
                dict_ds[scan_mode] = ds
            dt_cropped = xr.DataTree.from_dict(dict_ds)

            # Remap to common grid
            ds = dt_cropped.gpm.regrid_pmw_l1(scan_mode_reference=scan_modes[-1])

            # Add orbit mode
            ds = ds.assign_coords({"orbit_mode": get_orbit_mode(ds)})

            # Unstack variables with pmw_frequency and scan_mode dimension
            ds.update(ds["Tc"].gpm.unstack_dimension(dim="pmw_frequency", suffix="_"))
            ds.update(ds["sunGlintAngle"].gpm.unstack_dimension(dim="scan_mode", suffix="_"))
            ds.update(ds["incidenceAngle"].gpm.unstack_dimension(dim="scan_mode", suffix="_"))
            ds.update(ds["sunLocalTime"].gpm.unstack_dimension(dim="scan_mode", suffix="_"))
            ds.update(ds["Quality"].gpm.unstack_dimension(dim="scan_mode", suffix="_"))
            ds = ds.drop_vars(["Tc", "sunLocalTime", "Quality", "sunGlintAngle", "incidenceAngle"])

            # -----------------------------------------------------------------.
            #### Read data in memory
            ds = ds.compute()

            # -----------------------------------------------------------------.
            #### Convert to pandas dataframe
            df = ds.gpm.to_pandas_dataframe()
            list_df.append(df)

        # ---------------------------------------------------------------------.
        # Concatenate slices
        df = pd.concat(list_df)

        # ---------------------------------------------------------------------.
        # Close file connection
        dt.close()
        ds.close()
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
