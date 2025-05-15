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
"""This module implements tools for dataframe filtering."""
import datetime

import numpy as np
import polars as pl
import pyproj

from satbucket.checks import check_filepaths, check_start_end_time, get_current_utc_time
from satbucket.dataframe import (
    df_add_column,
    df_get_column,
    df_select_valid_rows,
)
from satbucket.info import get_info_from_filepath


def get_geodesic_distance_from_point(lons, lats, lon, lat):
    lons = np.asanyarray(lons)
    lats = np.asanyarray(lats)
    geod = pyproj.Geod(ellps="WGS84")
    _, _, distance = geod.inv(lons, lats, np.ones(lons.shape) * lon, np.ones(lats.shape) * lat, radians=False)
    return distance


def filter_around_point(df, lon, lat, distance):
    # https://stackoverflow.com/questions/76262681/i-need-to-create-a-column-with-the-distance-between-two-coordinates-in-polars
    # Retrieve coordinates
    lons = df_get_column(df, column="lon")
    lats = df_get_column(df, column="lat")
    # Compute geodesic distance
    distances = get_geodesic_distance_from_point(lons=lons, lats=lats, lon=lon, lat=lat)
    valid_indices = distances <= distance
    # Add distance
    df = df_add_column(df, column="distance", values=distances)
    # Select only valid rows
    df = df_select_valid_rows(df, valid_rows=valid_indices)
    return df


def filter_by_extent(df, extent, x="lon", y="lat"):
    if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
        df = df.filter(
            pl.col(x) >= extent[0],
            pl.col(x) <= extent[1],
            pl.col(y) >= extent[2],
            pl.col(y) <= extent[3],
        )
    else:  # pandas
        idx_valid = (df[x] >= extent[0]) & (df[x] <= extent[1]) & (df[y] >= extent[2]) & (df[y] <= extent[3])
        df = df.loc[idx_valid]
    return df


def apply_spatial_filters(df, filters=None):
    if filters is None:
        filters = {}
    if "extent" in filters:
        df = filter_by_extent(df, extent=filters["extent"], x="lon", y="lat")
    if "point_radius" in filters:
        lon, lat, distance = filters["point_radius"]
        df = filter_around_point(df, lon=lon, lat=lat, distance=distance)
    return df


def is_within_time_period(l_start_time, l_end_time, start_time, end_time):
    """Assess which files are within the start and end time."""
    # - Case 1
    #     s               e
    #     |               |
    #   ---------> (-------->)
    idx_select1 = np.logical_and(l_start_time <= start_time, l_end_time > start_time)
    # - Case 2
    #     s               e
    #     |               |
    #          ---------(-.)
    idx_select2 = np.logical_and(l_start_time >= start_time, l_end_time <= end_time)
    # - Case 3
    #     s               e
    #     |               |
    #                -------------
    idx_select3 = np.logical_and(l_start_time < end_time, l_end_time > end_time)
    # - Get idx where one of the cases occur
    idx_select = np.logical_or.reduce([idx_select1, idx_select2, idx_select3])
    return idx_select


def is_granule_within_time(start_time, end_time, file_start_time, file_end_time):
    """Check if a granule is within start_time and end_time."""
    # - Case 1
    #     s               e
    #     |               |
    #   ---------> (-------->)
    is_case1 = file_start_time <= start_time and file_end_time > start_time
    # - Case 2
    #     s               e
    #     |               |
    #          --------
    is_case2 = file_start_time >= start_time and file_end_time < end_time
    # - Case 3
    #     s               e
    #     |               |
    #                ------------->
    is_case3 = file_start_time < end_time and file_end_time > end_time
    # - Check if one of the conditions occurs
    return is_case1 or is_case2 or is_case3


def _filter_filepath(filepath, filename_pattern, start_time=None, end_time=None):
    """Check if a single filepath pass the filtering parameters.

    If do not match the filtering criteria, it returns ``None``.

    Parameters
    ----------
    filepath : str
        Filepath string.
    filename_pattern: int
        Filename pattern for extraction of time information.
    start_time : datetime.datetime
        Start time
        The default is ``None``.
    end_time : datetime.datetime
        End time.
        The default is ``None``.

    Returns
    -------
    filepaths : list
        Returns the filepaths subset.
        If no valid filepaths, return an empty list.

    """
    try:
        info_dict = get_info_from_filepath(filepath, filename_pattern)
    except ValueError:
        return None

    # Filter by start_time and end_time
    if start_time is not None and end_time is not None:
        file_start_time = info_dict["start_time"]
        file_end_time = info_dict["end_time"]
        if not is_granule_within_time(start_time, end_time, file_start_time, file_end_time):
            return None

    return filepath


def filter_filepaths(
    filepaths,
    filename_pattern,
    start_time=None,
    end_time=None,
):
    """Filter the Satellite filepaths based on specific parameters.

    Parameters
    ----------
    filepaths : list
        List of filepaths.
    filename_pattern: int
        Filename pattern for extraction of time information.
    start_time : datetime.datetime
        Start time
        The default is ``None``.
    end_time : datetime.datetime
        End time.
        The default is ``None``.

    Returns
    -------
    filepaths : list
        Returns the filepaths subset.
        If no valid filepaths, return an empty list.

    """
    # Check filepaths
    if isinstance(filepaths, type(None)):
        return []
    filepaths = check_filepaths(filepaths)
    if len(filepaths) == 0:
        return []

    # Check start_time and end_time
    if start_time is not None or end_time is not None:
        if start_time is None:
            start_time = datetime.datetime(1998, 1, 1, 0, 0, 0)  # Satellite start mission
        if end_time is None:
            end_time = get_current_utc_time()  # Current time
        start_time, end_time = check_start_end_time(start_time, end_time)

    # Filter filepaths
    filepaths = [
        _filter_filepath(
            filepath,
            start_time=start_time,
            end_time=end_time,
            filename_pattern=filename_pattern,
        )
        for filepath in filepaths
    ]
    # Remove None from the list
    return [filepath for filepath in filepaths if filepath is not None]
