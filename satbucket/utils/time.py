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
"""This module contains utilities for time processing."""

import numpy as np
import pandas as pd
import xarray as xr
from xarray.core import dtypes

from satbucket.checks import (
    check_start_end_time,
    check_time,
)

####--------------------------------------------------------------------------.
############################
#### Subsetting by time ####
############################


def subset_by_time(xr_obj, start_time=None, end_time=None):
    """Filter a Satellite xarray object by start_time and end_time.

    Parameters
    ----------
    xr_obj :
        A xarray object.
    start_time : datetime.datetime
        Start time.
        By default is ``None``
    end_time : datetime.datetime
        End time.
        By default is ``None``

    Returns
    -------
    xr_obj : xarray.DataArray or xarray.Dataset
        Satellite xarray object

    """
    # Check inputs
    if start_time is not None:
        start_time = check_time(start_time)
    if end_time is not None:
        end_time = check_time(end_time)
    if start_time is not None and end_time is not None:
        start_time, end_time = check_start_end_time(start_time, end_time)

    # Get 1D dimension of the coordinate
    dim_coords = list(xr_obj["time"].dims)

    # If no dimension, it means that nothing to be subsetted
    # - Likely the time dimension has been dropped ...
    if len(dim_coords) == 0:
        raise ValueError("Impossible to subset a non-dimensional time coordinate.")

    # If dimension > 1, not clear how to collapse to 1D the boolean array
    if len(dim_coords) != 1:
        raise ValueError(
            "Impossible to subset a non-dimensional time coordinate with dimension >=2.",
        )

    dim_coord = dim_coords[0]

    # Subset by start_time
    if start_time is not None:
        isel_bool = xr_obj["time"] >= pd.Timestamp(start_time)
        if not np.any(isel_bool):
            raise ValueError(f"No timesteps to return with start_time {start_time}.")
        isel_dict = {dim_coord: isel_bool}
        xr_obj = xr_obj.isel(isel_dict)

    # Subset by end_time
    if end_time is not None:
        isel_bool = xr_obj["time"] <= pd.Timestamp(end_time)
        if not np.any(isel_bool):
            raise ValueError(f"No timesteps to return with end_time {end_time}.")
        isel_dict = {dim_coord: isel_bool}
        xr_obj = xr_obj.isel(isel_dict)

    return xr_obj


def subset_by_time_slice(xr_obj, slice):
    start_time = slice.start
    end_time = slice.stop
    return subset_by_time(xr_obj, start_time=start_time, end_time=end_time)


####--------------------------------------------------------------------------.
##################################
#### Time Dimension Sanitizer ####
##################################


def is_nat(timesteps):
    """Return a boolean array indicating timesteps which are NaT."""
    # pd.isnull(np.datetime64('NaT'))
    # pd.isna(np.datetime64('NaT'))
    return pd.isna(timesteps)


def has_nat(timesteps):
    """Return True if any of the timesteps is NaT."""
    return np.any(is_nat(timesteps))


def interpolate_nat(timesteps, method="linear", limit=5, limit_direction=None, limit_area=None):
    """Fill NaT values using an interpolation method.

    For further information refers to :py:class:`pandas.DataFrame.interpolate`.

    Parameters
    ----------
    method : str
        Interpolation technique to use.
        ``'linear'``, the default, treat the timesteps as equally spaced.
    limit : int, optional
        Maximum number of consecutive NaTs to fill. Must be greater than 0.
    limit_direction : str, optional
        Valid values are ``'forward'``, ``'backward'`` and ``'both'``.
        Consecutive NaTs will be filled in this direction.
    limit_area : str, None
        Valid values are ``None``, ``'inside'`` and ``'outside'``,
        If limit is specified, consecutive NaTs will be filled with this restriction.
        * ``None``: No fill restriction.
        * 'inside': Only fill NaTs surrounded by valid values (interpolate).
        * 'outside': Only fill NaTs outside valid values (extrapolate).

    Notes
    -----
    Depending on the interpolation method (i.e. linear) the infilled values could have ns resolution.
    For further information refers to :py:class:`pandas.DataFrame.interpolate`.

    Returns
    -------
    timesteps:  numpy.ndarray
        Timesteps array of type datetime64[ns].

    """
    if len(timesteps) == 0:
        return timesteps
    # Retrieve input dtype
    timesteps_dtype = timesteps.dtype
    # Convert timesteps to float
    timesteps_num = timesteps.astype(float)
    # Convert NaT to np.nan
    timesteps_num[is_nat(timesteps)] = np.nan
    # Create pd.Series
    series = pd.Series(timesteps_num)
    # Estimate NaT value
    series = series.interpolate(
        method=method,
        limit=limit,
        limit_direction=limit_direction,
        limit_area=limit_area,
    )
    # Convert back to numpy array input dtype
    return series.to_numpy().astype(timesteps_dtype)


def infill_timesteps(timesteps, limit):
    """Infill missing timesteps if less than <limit> consecutive."""
    # Interpolate if maximum <limit> timesteps are missing
    timesteps = interpolate_nat(timesteps, method="linear", limit=limit, limit_area="inside")

    # Check if there are still residual NaT
    if np.any(is_nat(timesteps)):
        if len(timesteps) <= 2:
            error_message = "Not enough timesteps available to infill NaTs."
        elif is_nat(timesteps[0]) or is_nat(timesteps[-1]):
            error_message = "NaTs present at the beginning or at the end of the timesteps cannot be inferred."
        else:
            error_message = f"More than {limit} consecutive timesteps are NaT."
        raise ValueError(error_message)

    return timesteps


def ensure_time_validity(xr_obj, limit=10):
    """Attempt to correct the time coordinate if less than 'limit' consecutive NaT values are present.

    It raise a ValueError if more than consecutive NaT occurs.

    Parameters
    ----------
    xr_obj : xarray.DataArray or xarray.Dataset
        Satellite xarray object.

    Returns
    -------
    xr_obj : xarray.DataArray or xarray.Dataset
        Satellite xarray object.

    """
    attrs = xr_obj["time"].attrs
    timesteps = xr_obj["time"].to_numpy()
    timesteps = infill_timesteps(timesteps, limit=limit)
    if "time" not in list(xr_obj.dims):
        xr_obj["time"].data = timesteps
    else:
        xr_obj = xr_obj.assign_coords({"time": timesteps})
    xr_obj["time"].attrs = attrs
    return xr_obj


####--------------------------------------------------------------------------.


def get_dataset_start_end_time(ds: xr.Dataset, time_dim="time"):
    """Retrieves dataset starting and ending time.

    Parameters
    ----------
    ds : xarray.Dataset
        Input dataset
    time_dim: str
        Name of the time dimension.
        The default is "time".

    Returns
    -------
    tuple
        (``starting_time``, ``ending_time``)

    """
    starting_time = ds[time_dim].to_numpy()[0]
    ending_time = ds[time_dim].to_numpy()[-1]
    return (starting_time, ending_time)


def _define_fill_value(ds, fill_value):
    fill_value = {}
    for var in ds.data_vars:
        if np.issubdtype(ds[var].dtype, np.floating):
            fill_value[var] = dtypes.NA
        elif np.issubdtype(ds[var].dtype, np.integer):
            if "_FillValue" in ds[var].attrs:
                fill_value[var] = ds[var].attrs["_FillValue"]
            else:
                fill_value[var] = np.iinfo(ds[var].dtype).max
    return fill_value


def _check_time_sorted(ds, time_dim):
    time_diff = np.diff(ds[time_dim].data.astype(int))
    if np.any(time_diff == 0):
        raise ValueError(f"In the {time_dim} dimension there are duplicated timesteps !")
    if not np.all(time_diff > 0):
        print(f"The {time_dim} dimension was not sorted. Sorting it now !")
        ds = ds.sortby(time_dim)
    return ds


def regularize_dataset(
    ds: xr.Dataset,
    freq: str,
    time_dim: str = "time",
    method: str | None = None,
    fill_value=None,
):
    """Regularize a dataset across time dimension with uniform resolution.

    Parameters
    ----------
    ds : xarray.Dataset
        xarray Dataset.
    time_dim : str, optional
        The time dimension in the xarray.Dataset. The default is ``"time"``.
    freq : str
        The ``freq`` string to pass to `pd.date_range()` to define the new time coordinates.
        Examples: ``freq="2min"``.
    method : str, optional
        Method to use for filling missing timesteps.
        If ``None``, fill with ``fill_value``. The default is ``None``.
        For other possible methods, see xarray.Dataset.reindex()`.
    fill_value : (float, dict), optional
        Fill value to fill missing timesteps.
        If not specified, for float variables it uses ``dtypes.NA`` while for
        for integers variables it uses the maximum allowed integer value or,
        in case of undecoded variables, the ``_FillValue`` DataArray attribute..

    Returns
    -------
    ds_reindexed : xarray.Dataset
        Regularized dataset.

    """
    ds = _check_time_sorted(ds, time_dim=time_dim)
    start_time, end_time = get_dataset_start_end_time(ds, time_dim=time_dim)
    new_time_index = pd.date_range(
        start=pd.to_datetime(start_time),
        end=pd.to_datetime(end_time),
        freq=freq,
    )

    # Define fill_value dictionary
    if fill_value is None:
        fill_value = _define_fill_value(ds, fill_value)

    # Regularize dataset and fill with NA values
    ds = ds.reindex(
        {time_dim: new_time_index},
        method=method,  # do not fill gaps
        # tolerance=tolerance,  # mismatch in seconds
        fill_value=fill_value,
    )
    return ds
