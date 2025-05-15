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
"""This module contains functions to check the sat-bucket arguments."""
import datetime
import sys

import numpy as np


def get_current_utc_time():
    if sys.version_info >= (3, 11):
        return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    return datetime.datetime.utcnow()


def check_time(time):
    """Check time validity.

    It returns a :py:class:`datetime.datetime` object to seconds precision.

    Parameters
    ----------
    time : datetime.datetime, datetime.date, numpy.datetime64 or str
        Time object.
        Accepted types: ``datetime.datetime``, ``datetime.date``, ``numpy.datetime64`` or ``str``.
        If string type, it expects the isoformat ``YYYY-MM-DD hh:mm:ss``.

    Returns
    -------
    time: datetime.datetime

    """
    if not isinstance(time, (datetime.datetime, datetime.date, np.datetime64, np.ndarray, str)):
        raise TypeError(
            "Specify time with datetime.datetime objects or a " "string of format 'YYYY-MM-DD hh:mm:ss'.",
        )

    # If numpy array with datetime64 (and size=1)
    if isinstance(time, np.ndarray):
        if np.issubdtype(time.dtype, np.datetime64):
            if time.size == 1:
                time = time[0].astype("datetime64[s]").tolist()
            else:
                raise ValueError("Expecting a single timestep!")
        else:
            raise ValueError("The numpy array does not have a numpy.datetime64 dtype!")

    # If np.datetime64, convert to datetime.datetime
    if isinstance(time, np.datetime64):
        time = time.astype("datetime64[s]").tolist()
    # If datetime.date, convert to datetime.datetime
    if not isinstance(time, (datetime.datetime, str)):
        time = datetime.datetime(time.year, time.month, time.day, 0, 0, 0)
    if isinstance(time, str):
        try:
            time = datetime.datetime.fromisoformat(time)
        except ValueError:
            raise ValueError("The time string must have format 'YYYY-MM-DD hh:mm:ss'")
    # If datetime object carries timezone that is not UTC, raise error
    if time.tzinfo is not None:
        if str(time.tzinfo) != "UTC":
            raise ValueError("The datetime object must be in UTC timezone if timezone is given.")
        # If UTC, strip timezone information
        time = time.replace(tzinfo=None)
    return time


def check_start_end_time(start_time, end_time):
    """Check start_time and end_time value validity."""
    start_time = check_time(start_time)
    end_time = check_time(end_time)

    # Check start_time and end_time are chronological
    if start_time > end_time:
        raise ValueError("Provide 'start_time' occurring before of 'end_time'.")
    # Check start_time and end_time are in the past
    if start_time > get_current_utc_time():
        raise ValueError("Provide 'start_time' occurring in the past.")
    if end_time > get_current_utc_time():
        raise ValueError("Provide 'end_time' occurring in the past.")
    return (start_time, end_time)


def check_filepaths(filepaths):
    """Ensure filepaths is a list of string."""
    if isinstance(filepaths, str):
        filepaths = [filepaths]
    if not isinstance(filepaths, list):
        raise TypeError("Expecting a list of filepaths.")
    return filepaths
