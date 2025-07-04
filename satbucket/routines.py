# -----------------------------------------------------------------------------.
# MIT License

# Copyright (c) 2024 sat-bucket developers
#
# This file is part of sat-bucket.

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# -----------------------------------------------------------------------------.
"""This module provides the routines for the creation of Satellite Geographic Buckets."""
import os

import dask
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset
import pyarrow.parquet as pq
from tqdm import tqdm

from satbucket.checks import check_start_end_time
from satbucket.filters import filter_filepaths, is_within_time_period
from satbucket.info import get_start_end_time_from_filepaths
from satbucket.io import (
    get_bucket_spatial_partitioning,
    get_bucket_temporal_partitioning,
    get_exisiting_partitions_paths,
    get_filepaths_within_paths,
    write_bucket_info,
)
from satbucket.utils.dask import clean_memory, get_client
from satbucket.utils.directories import get_first_file, list_and_filter_files
from satbucket.utils.parallel import compute_list_delayed
from satbucket.utils.timing import print_task_elapsed_time
from satbucket.writers import (
    preprocess_writer_kwargs,
    write_dataset_metadata,
    write_partitioned_dataset,
)

####--------------------------------------------------------------------------------------------------.
#### Bucket Granules


def split_list_in_blocks(values, block_size):
    return [values[i : i + block_size] for i in range(0, len(values), block_size)]


def write_granule_bucket(
    src_filepath,
    bucket_dir,
    spatial_partitioning,
    granule_to_df_func,
    x="lon",
    y="lat",
    # Writer kwargs
    **writer_kwargs,
):
    """Write a geographically partitioned Parquet Dataset of a Satellite granules.

    Parameters
    ----------
    src_filepath : str
        File path of the granule to store in the bucket archive.
    bucket_dir: str
        Base directory of the per-granule bucket archive.
    spatial_partitioning: satbucket.SpatialPartitioning
        A spatial partitioning class.
    granule_to_df_func : Callable
        Function taking a granule filepath, opening it and returning a pandas or dask dataframe.
    x: str
        The name of the x column. The default is "lon".
    y: str
        The name of the y column. The default is "lat".
    **writer_kwargs: dict
        Optional arguments to be passed to the pyarrow Dataset Writer.
        Common arguments are 'format' and 'use_threads'.
        The default file ``format`` is ``'parquet'``.
        The default ``use_threads`` is ``True``, which enable multithreaded file writing.
        More information available at https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html

    """
    # Define unique prefix name so to add files to the bucket archive
    # - This prevent risk of overwriting
    # - If df is pandas.dataframe -->  f"{filename_prefix}_" + "{i}.parquet"
    # - if df is a dask.dataframe -->  f"{filename_prefix}_dask.partition_{part_index}"
    filename_prefix = os.path.splitext(os.path.basename(src_filepath))[0]

    # Retrieve dataframe
    df = granule_to_df_func(src_filepath)

    # Do nothing if returns None
    # - For example when no intersecting AOI
    if df is None:
        return

    # Add partitioning columns
    df = spatial_partitioning.add_labels(df=df, x=x, y=y)

    # Write partitioned dataframe
    write_partitioned_dataset(
        df=df,
        base_dir=bucket_dir,
        filename_prefix=filename_prefix,
        partitions=spatial_partitioning.order,
        partitioning_flavor=spatial_partitioning.flavor,
        **writer_kwargs,
    )


def _try_write_granule_bucket(**kwargs):
    try:
        # synchronous
        with dask.config.set(scheduler="single-threaded"):
            write_granule_bucket(**kwargs)
            # If works, return None
            info = None
    except Exception as e:
        # Define tuple to return
        src_filepath = kwargs["src_filepath"]
        info = src_filepath, str(e)
    return info


@print_task_elapsed_time(prefix="Granules Bucketing Operation Terminated.")
def write_granules_bucket(
    filepaths,
    # Bucket configuration
    bucket_dir,
    spatial_partitioning,
    granule_to_df_func,
    # Processing options
    parallel=True,
    max_concurrent_tasks=None,
    max_dask_total_tasks=500,
    # Writer kwargs
    use_threads=False,
    row_group_size="500MB",
    **writer_kwargs,
):
    """Write a geographically partitioned Parquet Dataset of Satellite granules.

    Parameters
    ----------
    filepaths : str
        File paths of the Satellite granules to store in the bucket archive.
    bucket_dir: str
        Base directory of the per-granule bucket archive.
    spatial_partitioning: satbucket.SpatialPartitioning
        A spatial partitioning class.
        Carefully consider the size of the partitions.
        Earth partitioning by:
        - 1° degree corresponds to 64800 directories (360*180)
        - 5° degree corresponds to 2592 directories (72*36)
        - 10° degree corresponds to 648 directories (36*18)
        - 15° degree corresponds to 288 directories (24*12)
    granule_to_df_func : callable
        Function taking a granule filepath, opening it and returning a pandas or dask dataframe.
    parallel : bool
        Whether to bucket several granules in parallel.
        The default is ``True``.
    max_concurrent_tasks : int
        The maximum number of Dask tasks to be concurrently executed.
        If ``None``, let the Dask Scheduler to choose.
        The default is ``None``.
    max_dask_total_tasks : int
        The maximum number of Dask tasks to be scheduled.
        The default is 500.
    use_threads : bool
        Whether to write Parquet files with multiple threads.
        If bucketing granules in a multiprocessing environment, it's better to
        set it to ``False``.
        The default is ``False``.
    row_group_size : int or str, optional
        Maximum number of rows in each written Parquet row group.
        If specified as a string (i.e. "500 MB"), the equivalent row group size
        number is estimated. The default is "500MB".
    **writer_kwargs: dict
        Optional arguments to be passed to the pyarrow Dataset Writer.
        Common arguments are ``format`` and ``use_threads``.
        The default file ``format`` is ``parquet``.
        The default ``use_threads`` is ``True``, which enable multithreaded file writing.
        More information available at https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html

    """
    # Define flavor of directory partitioning
    writer_kwargs["row_group_size"] = row_group_size
    writer_kwargs["use_threads"] = use_threads

    # Write down the information of the bucket
    write_bucket_info(bucket_dir=bucket_dir, spatial_partitioning=spatial_partitioning)

    # Split long list of files in blocks
    list_blocks = split_list_in_blocks(filepaths, block_size=max_dask_total_tasks)

    # Execute tasks by blocks to avoid dask overhead
    n_blocks = len(list_blocks)

    for i, block_filepaths in enumerate(list_blocks):
        print(f"Executing tasks block {i+1}/{n_blocks}")

        # Loop over granules
        func = dask.delayed(_try_write_granule_bucket) if parallel else _try_write_granule_bucket

        list_results = [
            func(
                src_filepath=src_filepath,
                bucket_dir=bucket_dir,
                spatial_partitioning=spatial_partitioning,
                granule_to_df_func=granule_to_df_func,
                # Writer kwargs
                **writer_kwargs,
            )
            for src_filepath in block_filepaths
        ]

        # If delayed, execute the tasks
        if parallel:
            list_results = compute_list_delayed(
                list_results,
                max_concurrent_tasks=max_concurrent_tasks,
            )

        # Process results to detect errors
        list_errors = [error_info for error_info in list_results if error_info is not None]
        for src_filepath, error_str in list_errors:
            print(f"An error occurred while processing {src_filepath}: {error_str}")

        # If parallel=True, retrieve client, clean the memory and restart
        if parallel:
            client = get_client()
            clean_memory(client)
            client.restart()


####--------------------------------------------------------------------------------------------------.
#### Bucket DataFrame
@print_task_elapsed_time(prefix="Dataset Bucket Operation Terminated.")
def write_bucket(
    df,
    bucket_dir,
    spatial_partitioning,
    x="lon",
    y="lat",
    # Writer arguments
    filename_prefix="part",
    row_group_size="500MB",
    **writer_kwargs,
):
    """
    Write a geographically partitioned Parquet Dataset.

    Parameters
    ----------
    df : pandas.DataFrame or dask.dataframe.DataFrame
        Pandas or Dask dataframe to be written into a geographic bucket.
    bucket_dir: str
        Base directory of the geographic bucket archive.
    spatial_partitioning: satbucket.SpatialPartitioning
        A spatial partitioning class.
        Carefully consider the size of the partitions.
        Earth partitioning by:
        - 1° degree corresponds to 64800 directories (360*180)
        - 5° degree corresponds to 2592 directories (72*36)
        - 10° degree corresponds to 648 directories (36*18)
        - 15° degree corresponds to 288 directories (24*12)
    x: str
        The name of the x column. The default is "lon".
    y: str
        The name of the y column. The default is "lat".
    row_group_size : int or str, optional
        Maximum number of rows in each written Parquet row group.
        If specified as a string (i.e. "500 MB"), the equivalent row group size
        number is estimated. The default is "500MB".
    **writer_kwargs: dict
        Optional arguments to be passed to the pyarrow Dataset Writer.
        Common arguments are 'format' and 'use_threads'.
        The default file ``format`` is ``'parquet'``.
        The default ``use_threads`` is ``True``, which enable multithreaded file writing.
        More information available at https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html

    """
    # Write down the information of the bucket
    write_bucket_info(
        bucket_dir=bucket_dir,
        spatial_partitioning=spatial_partitioning,
    )

    # Add partitioning columns
    df = spatial_partitioning.add_labels(df=df, x=x, y=y)

    # Write bucket
    writer_kwargs["row_group_size"] = row_group_size
    write_partitioned_dataset(
        df=df,
        base_dir=bucket_dir,
        partitions=spatial_partitioning.order,
        partitioning_flavor=spatial_partitioning.flavor,
        filename_prefix=filename_prefix,
        **writer_kwargs,
    )


####--------------------------------------------------------------------------------------------------.
#### Merge Granules


def check_temporal_partitioning(temporal_partitioning):
    """Check validity of temporal_partitioning argument."""
    valid_values = ["year", "month", "season", "quarter"]
    if not isinstance(temporal_partitioning, str):
        raise TypeError("'temporal_partitioning' must be a string.")
    if temporal_partitioning not in valid_values:
        raise ValueError(f"Invalid '{temporal_partitioning}'. Valid values are {valid_values}")
    return temporal_partitioning


def define_dict_partitions(src_bucket_dir, src_spatial_partitioning, dst_spatial_partitioning=None):
    """List source partitions directories paths for each destination partition."""
    if dst_spatial_partitioning is not None:
        raise NotImplementedError("Repartitioning not yet implemented.")
    dst_spatial_partitioning = src_spatial_partitioning

    # Retrieve path to source partitions
    n_levels = dst_spatial_partitioning.n_levels
    dir_trees = dst_spatial_partitioning.directories
    partitions_paths = get_exisiting_partitions_paths(src_bucket_dir, dir_trees)  # on 4096 directories ...
    # Define list of destination bucket partitions and source bucket directories
    sep = os.path.sep
    dict_partitions = {sep.join(path.strip(sep).split(sep)[-n_levels:]): [path] for path in partitions_paths}
    return dict_partitions


def get_template_table(dict_partitions):
    """Read and return a table template."""
    # Take the first file in the partitions
    file_found = False
    for list_src_dir in dict_partitions.values():
        for path in list_src_dir:
            filepath = get_first_file(path)
            if filepath is not None:
                file_found = True
                break  # should break all loops
        if file_found:
            break

    if not file_found:
        raise ValueError("No file found in the source bucket archive.")

    # Read the first parquet file found
    template_table = pq.read_table(filepath)
    return template_table


def get_time_prefix(timestep, temporal_partitioning):
    """Define a time prefix string from a datetime object."""
    if temporal_partitioning == "year":
        return f"{timestep.year}"  # e.g. "2021"
    if temporal_partitioning == "month":
        return f"{timestep.year}_{timestep.month}"  # e.g. "2021_1"
    if temporal_partitioning == "quarter":
        # Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
        quarter = (timestep.month - 1) // 3 + 1
        return f"{timestep.year}_{quarter}"  # e.g. "2021_1" for Q1 2021
    if temporal_partitioning == "day":
        return f"{timestep.year}_{timestep.month}_{timestep.day}"
    raise NotImplementedError(f"Invalid '{temporal_partitioning}' temporal_partitioning")


def get_partitioning_boundaries(start_time, end_time, temporal_partitioning):
    """Define a time prefix string from a datetime object."""
    # --------
    # YEAR
    if temporal_partitioning == "year":
        if end_time != pd.Timestamp(f"{end_time.year}-01-01 00:00:00"):
            end_time = end_time + pd.DateOffset(years=1)
        boundaries = pd.date_range(
            start=pd.Timestamp(f"{start_time.year}-01-01"),
            end=pd.Timestamp(f"{end_time.year}-01-01"),
            freq="YS",
        )
        return boundaries

    # --------
    # MONTH
    if temporal_partitioning == "month":
        if end_time != pd.Timestamp(f"{end_time.year}-{end_time.month:02d}-01 00:00:00"):
            end_time = end_time + pd.DateOffset(months=1)
        boundaries = pd.date_range(
            start=pd.Timestamp(f"{start_time.year}-{start_time.month}-01 00:00:00"),
            end=pd.Timestamp(f"{end_time.year}-{end_time.month}-01 00:00:00"),
            freq="MS",
        )
        return boundaries

    # --------
    # QUARTER
    # Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec
    if temporal_partitioning == "quarter":
        # Define start time quarter month
        start_quarter_idx = (start_time.month - 1) // 3 + 1
        start_quarter_start_month = 3 * (start_quarter_idx - 1) + 1
        # Define end time quarter month
        end_quarter_idx = (end_time.month - 1) // 3 + 1
        end_quarter_start_month = 3 * (end_quarter_idx - 1) + 1
        # Update to next quarter if necessary
        if end_time != pd.Timestamp(f"{end_time.year}-{end_quarter_start_month:02d}-01 00:00:00"):
            end_time = end_time + pd.DateOffset(months=3)
            end_quarter_idx = (end_time.month - 1) // 3 + 1
            end_quarter_start_month = 3 * (end_quarter_idx - 1) + 1
        boundaries = pd.date_range(
            start=pd.Timestamp(f"{start_time.year}-{start_quarter_start_month}-01 00:00:00"),
            end=pd.Timestamp(f"{end_time.year}-{end_quarter_start_month}-01 00:00:00"),
            freq="QS",
        )
        return boundaries

    # --------
    # DAY
    if temporal_partitioning == "day":
        if end_time != end_time.normalize():
            end_time = end_time.normalize() + pd.DateOffset(days=1)
        boundaries = pd.date_range(
            start=pd.Timestamp(f"{start_time.year}-{start_time.month}-{start_time.day} 00:00:00"),
            end=end_time,
            freq="D",
        )
        return boundaries
    raise NotImplementedError(f"Invalid '{temporal_partitioning}' temporal_partitioning")


def get_list_group_periods(start_time, end_time, temporal_partitioning):
    """List group time periods."""
    # Retrieve group time boundaries
    boundaries = get_partitioning_boundaries(start_time, end_time, temporal_partitioning)

    # Define list with group time information
    intervals = []
    for i, group_start in enumerate(boundaries):
        # Retrieve group end time
        group_end = boundaries[i + 1] if i < len(boundaries) - 1 else end_time

        # Clamp the boundaries to the overall [start_time, end_time)
        group_start = max(group_start, start_time)
        group_end = min(group_end, end_time)

        # Build time prefix (e.g., "2021", "2021_1", "2021_1_15", etc.)
        time_prefix = get_time_prefix(group_start, temporal_partitioning)

        # Avoid zero-length intervals
        if group_start < group_end:
            intervals.append((time_prefix, group_start, group_end))

    return intervals


def group_files_by_time(filepaths, start_time, end_time, temporal_partitioning, filename_pattern):
    # Convert to numpy array
    filepaths = np.array(filepaths)

    # Retrieve start time and end_time of each file
    l_start_time, l_end_time = get_start_end_time_from_filepaths(filepaths, filename_pattern=filename_pattern)

    # Initialize start_time and end_time if None
    if start_time is None:
        start_time = pd.Timestamp(l_start_time.min())
    if end_time is None:
        end_time = pd.Timestamp(l_end_time.max())

    # Define possible group_start_time and group_end_time
    list_group_periods = get_list_group_periods(start_time, end_time, temporal_partitioning)

    # List all filepaths for each time group
    groups_dict = {}
    # group_key, group_start_time, group_end_time = list_group_periods[2]
    for group_key, group_start_time, group_end_time in list_group_periods:
        is_within_group = is_within_time_period(
            l_start_time=l_start_time,
            l_end_time=l_end_time,
            start_time=group_start_time,
            end_time=group_end_time,
        )
        list_group_filepaths = filepaths[is_within_group]
        if len(list_group_filepaths) > 0:
            groups_dict[group_key] = (group_start_time, group_end_time, list_group_filepaths)

    return groups_dict


def define_dataset_filter(
    start_time,
    end_time,
    # dst_spatial_partitioning, partition_label or extent
):

    time_filter = (pyarrow.dataset.field("time") >= start_time) & (pyarrow.dataset.field("time") < end_time)

    # Create the spatial filter for longitude and latitude
    # lon_min, lon_max, lat_min, lat_max = -180, 180, -90, 90
    # location_filter = (
    #     (pyarrow.dataset.field("lon") >= lon_min) & (pyarrow.dataset.field("lon") <= lon_max) &
    #     (pyarrow.dataset.field("lat") >= lat_min) & (pyarrow.dataset.field("lat") <= lat_max)
    # )

    # Combine both filters with an AND operation
    # dataset_filter = time_filter & location_filter

    dataset_filter = time_filter
    return dataset_filter


@print_task_elapsed_time(prefix="Bucket Merging Terminated.")
def merge_granule_buckets(
    src_bucket_dir,
    dst_bucket_dir,
    filename_pattern,
    # Bucket structure
    dst_spatial_partitioning=None,
    temporal_partitioning="year",
    # Update options
    start_time=None,
    end_time=None,
    update=False,
    # Parquet options
    row_group_size="200MB",
    max_file_size="2GB",
    compression="snappy",
    compression_level=None,
    write_metadata=False,
    write_statistics=False,
    # Computing options
    max_open_files=0,
    use_threads=True,
    # Scanner options
    batch_size=131_072,
    batch_readahead=10,
    fragment_readahead=20,
):
    """Merge the per-granule bucket archive in a single optimized archive.

    Set ulimit -n 999999 before running this routine !

    If you use ``write_metadata=True``, the archive can't currently be updated !

    Parameters
    ----------
    src_bucket_dir : str
        Base directory of the per-granule bucket archive.
    dst_bucket_dir : str
        Directory path of the final bucket archive.
    start_time : datetime.datetime
        Start time of the file to consolidate.
        The default is ``None``.
    end_time : datetime.datetime
        Non-inclusive end time of the file to consolidate.
        The default is ``None``.
    temporal_partitioning:
        Define the temporal partitions over which to groups files together.
        Only to be defined for a new bucket archive.
        Valid values are "year", "month", "season", "quarter" or "day".
        The default value is "year".
        If ``update=True``, use the temporal partitions of the existing bucket archive.
    update : bool
        Whether to update an existing bucket archive with new data.
        Make sure to specify start_time and end_time covering the time period of
        groups to avoid loss of data. If grouping by "year", specify start_time
        to be on January first at 00:00:00.
        If grouping by year and months,
        specify the start time to be the first day of the month at 00:00:00.
        The default is ``False``.
    write_metadata: bool,
        The default is ``False``.
        Collect in a single metadata file all row groups statistics of all files.
        If ``True``, slow down a lot the routine.
        If you use ``write_metadata=True``, the archive can't currently be updated !
    write_statistics: bool or list
        Whether to compute and include column-level statistics (such as minimum, maximum, and null counts)
        in the metadata of each row group for all or some specific columns.
        Row group statistics allow for some more efficient queries as they might
        allow skipping irrelevant row groups or reading of entire files.
        If ``True`` (or some columns are specified), the routine can take much longer to execute !
        The default is ``False``
    row_group_size : int or str, optional
        Maximum number of rows to be written in each Parquet row group.
        If specified as a string (i.e. ``"200 MB"``), the equivalent number of rows is estimated.
        The default is ``"200MB"``.
    max_file_size: str, optional
        Maximum number of rows to be written in a Parquet file.
        If specified as a string, the equivalent number of rows is estimated.
        Ideally the value should be a multiple of ``row_group_size``.
        The default is ``"2GB"``.
    compression : str, optional
        Specify the compression codec, either on a general basis or per-column.
        Valid values: ``{"none", "snappy", "gzip", "brotli", "lz4", "zstd"}``.
        The default is ``snappy``.
    compression_level : int or dict, optional
        Specify the compression level for a codec, either on a general basis or per-column.
        If ``None`` is passed, arrow selects the compression level for the compression codec in use.
        The compression level has a different meaning for each codec, so you have
        to read the pyArrow documentation of the codec you are using at
        https://arrow.apache.org/docs/python/generated/pyarrow.Codec.html
        The default is ``None``.
    max_open_files, int, optional
        If greater than 0 then this will limit the maximum number of files that can be left open.
        If an attempt is made to open too many files then the least recently used file will be closed.
        If this setting is set too low you may end up fragmenting your data into many small files.
        The default is ``0``.
        Note that Linux has a default limit of ``1024``. Before starting the python session,
        increase it with ``ulimit -n <new_much_higher_limit>``.
    use_threads: bool, optional
        If enabled, then maximum parallelism will be used to read and write files (in multithreading).
        The number of threads is determined by the number of available CPU cores.
        The default is ``True``.
    batch_size : int
        Maximum number of rows per record batch produced by the dataset scanner.
        For concatenating small files (each typically a single fragment with one row group),
        set batch_size larger than the number of rows of the small files so that data from multiple
        files can be aggregated into a single batch. This helps reduce per-batch overhead.
        If scanned record batches are overflowing memory then this value can be reduced to reduce the memory usage.
        The default value is ``131_072``.
    batch_readahead : int
        The number of batches to prefetch asynchronously from an open file.
        Increasing this number will increase RAM usage but could also improve IO utilization.
        When each file contains a single row group (and thus only one batch), the benefit of
        batch_readahead is limited. In such cases, a lower value is generally sufficient.
        The default is ``10``.
    fragment_readahead : int
        The number of individual small files to prefetch concurrently.
        Increasing this number will increase RAM usage but could also improve IO utilization.
        Prefetching multiple fragments concurrently helps hide the latency of opening and reading each file.
        The default is ``20``.

    Recommendations
    ---------------
    - For small files with a single row group, ensure that batch_size exceeds the number of rows
      in any individual file to allow efficient aggregation.
    - Focus on tuning fragment_readahead to prefetch multiple files simultaneously, as this yields
      greater performance benefits than batch_readahead in this context.
    - Adjust these parameters based on system memory and available threads; while they operate
      asynchronously, excessively high values may oversubscribe system resources without further gains.

    Returns
    -------
    None.

    """
    # Check arguments for update=True
    if update:
        # Check destination bucket dir exists if update=True
        if not os.path.exists(dst_bucket_dir):
            raise OSError(f"The bucket directory {dst_bucket_dir} does not exists.")

        # Check write_metadata argument
        if write_metadata:
            raise NotImplementedError("If update=True, it is currently not possible to update the metadata.")

        # Check start_time and end_time
        if start_time is None or end_time is None:
            raise ValueError("Define both 'start_time' and 'end_time' if update=True.")
        start_time, end_time = check_start_end_time(start_time, end_time)

    # Retrieve source spatial partitioning
    src_spatial_partitioning = get_bucket_spatial_partitioning(bucket_dir=src_bucket_dir)

    # Retrieve destination spatial partitioning
    if update:
        dst_spatial_partitioning = get_bucket_spatial_partitioning(bucket_dir=dst_bucket_dir)
        temporal_partitioning = get_bucket_temporal_partitioning(bucket_dir=dst_bucket_dir)
    elif dst_spatial_partitioning is None:
        dst_spatial_partitioning = src_spatial_partitioning
    else:
        raise NotImplementedError("Repartitioning not implemented yet.")

    # Check temporal partitioning
    check_temporal_partitioning(temporal_partitioning)

    # Identify destination partitions
    # - Output: {dst_partition_tree: [src_partition_path, src_partition_path]}
    # - Currently we assume same partitioning between source and destination
    # TODO:
    # - Spatial Repartitioning is not yet implemented
    # - group/split src partition paths for desired dst partitions
    # - Perform filtering on lon and lat in dataset scanner by dst_spatial_partitioning bounds
    dict_partitions = define_dict_partitions(
        src_bucket_dir=src_bucket_dir,
        src_spatial_partitioning=src_spatial_partitioning,
        dst_spatial_partitioning=None,
    )  # TODO: not yet implemented

    n_partitions = len(dict_partitions)
    print(f"{n_partitions} geographic partitions to process.")

    # Write bucket info
    if not update:
        write_bucket_info(
            bucket_dir=dst_bucket_dir,
            spatial_partitioning=dst_spatial_partitioning,
            temporal_partitioning=temporal_partitioning,
        )

    # -----------------------------------------------------------------------------------------------.
    # Retrieve table schema
    # - spatial_partitioning.levels are read by pq.read_table as dictionaries (depending on pyarrow version)
    # - spatial_partitioning.levels columns must be dropped by the table if present
    template_table = get_template_table(dict_partitions)
    if np.all(np.isin(dst_spatial_partitioning.levels, template_table.column_names)):
        template_table = template_table.drop_columns(dst_spatial_partitioning.levels)
    schema = template_table.schema

    # Define writer_kwargs
    writer_kwargs = {}
    writer_kwargs["row_group_size"] = row_group_size
    writer_kwargs["max_file_size"] = max_file_size
    writer_kwargs["compression"] = compression
    writer_kwargs["compression_level"] = compression_level
    writer_kwargs["max_open_files"] = max_open_files
    writer_kwargs["use_threads"] = use_threads
    writer_kwargs["write_metadata"] = write_metadata
    writer_kwargs["write_statistics"] = write_statistics
    writer_kwargs, metadata_collector = preprocess_writer_kwargs(
        writer_kwargs=writer_kwargs,
        df=template_table,
    )

    # -----------------------------------------------------------------------------------------------.
    # Concatenate data within bins
    # - Cannot rewrite directly the full pyarrow.dataset because there is no way to specify when
    #    data from each partition have been scanned completely (and can be written to disk)
    print("Start concatenating the granules bucket archive")

    n_partitions = len(dict_partitions)
    # partition_label = list(dict_partitions)[0]
    # list_src_partition_dir = dict_partitions[partition_label]
    for partition_label, list_src_partition_dir in tqdm(dict_partitions.items(), total=n_partitions):
        # Retrieve all available filepaths (sorted)
        filepaths = get_filepaths_within_paths(
            paths=list_src_partition_dir,
            parallel=False,
            file_extension=".parquet",
            glob_pattern=None,
            regex_pattern=None,
        )
        # Filter by time window
        if start_time is not None and end_time is not None:
            filepaths = filter_filepaths(
                filepaths,
                start_time=start_time,
                end_time=end_time,
                filename_pattern=filename_pattern,
            )

        # Check file left
        if len(filepaths) == 0:
            print(f"No data to consolidate for partition {partition_label}.")
            continue

        # Define destination partition directory
        dst_partition_dir = os.path.join(dst_bucket_dir, partition_label)

        # If the directory already exists and update=False, raise an error !
        if not update and os.path.exists(dst_partition_dir):
            raise ValueError(f"The partition {partition_label} already exists. Use 'update=True' to update an archive.")

        # Define groups to create
        groups_dict = group_files_by_time(
            filepaths=filepaths,
            start_time=start_time,
            end_time=end_time,
            filename_pattern=filename_pattern,
            temporal_partitioning=temporal_partitioning,
        )

        # If update=True, remove archived destination files of groups to be updated
        if update:
            # Ensure partition directory exists
            os.makedirs(dst_partition_dir, exist_ok=True)

            # Retrieve path of existing files in the destination archive
            existing_filepaths = list_and_filter_files(
                path=dst_partition_dir,
                file_extension=".parquet",
                glob_pattern=None,
                regex_pattern=None,
                sort=False,  # small speed up
            )

            # Remove files that starts with the same time_prefix (old file)
            # - Examples of groups_dict keys: 2021_1 when groups=["year", "month"]
            # - Example of filename: 2021_1_{i}.parquet where {i} is an id given by pyarrow parquet
            for time_prefix in list(groups_dict):
                for filepath in existing_filepaths:
                    if os.path.basename(filepath).startswith(time_prefix):
                        os.remove(filepath)

        # Save a consolidated parquet by the specified time group
        for time_prefix, (group_start_time, group_end_time, src_filepaths) in groups_dict.items():

            # Define filename pattern
            basename_template = f"{time_prefix}_" + "{i}.parquet"

            # Define pyarrow.Expression to filter rows based on time and geolocation/geometry
            # - TODO: geolocation filter when repartitioning
            dataset_filter = define_dataset_filter(
                start_time=group_start_time,
                end_time=group_end_time,
                # dst_spatial_partitioning, partition_label or extent
            )

            # Read Dataset
            dataset = pyarrow.dataset.dataset(list(src_filepaths), format="parquet")

            # Define scanner
            scanner = dataset.scanner(
                batch_size=batch_size,
                batch_readahead=batch_readahead,
                fragment_readahead=fragment_readahead,
                use_threads=use_threads,
                filter=dataset_filter,
            )

            # Rewrite dataset
            pa.dataset.write_dataset(
                scanner,
                base_dir=dst_partition_dir,
                basename_template=basename_template,
                # Directory options
                create_dir=True,
                existing_data_behavior="overwrite_or_ignore",
                # Options
                **writer_kwargs,
            )

    # Write metadata if asked
    if write_metadata and metadata_collector:
        write_dataset_metadata(base_dir=dst_bucket_dir, metadata_collector=metadata_collector, schema=schema)
