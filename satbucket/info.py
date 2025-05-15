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
"""This module implements tools to extract information from file names."""
import datetime
import os

import numpy as np
from trollsift import Parser


def parse_filename_pattern(filename, pattern):
    p = Parser(pattern)
    info_dict = p.parse(filename)

    # Check start_time is available
    if "start_time" not in info_dict:
        raise ValueError("Missing start_time information.")

    # Retrieve start_time information
    start_time = info_dict.get("start_time")
    if start_time.year == 1900:  # no date provided
        if "start_date" not in info_dict:
            raise ValueError("start_time is a time object but start_date is missing or invalid.")
        start_date = info_dict.get("start_date").date()
        start_time = datetime.datetime.combine(start_date, start_time.time())

    # If end_time is not available assume start_time + 2h
    if "end_time" not in info_dict:
        end_time = start_time + datetime.timedelta(hours=2)
    else:  # Retrieve end_time information
        end_time = info_dict.get("end_time")
        if end_time.year == 1900:  # no date provided
            if "end_date" in info_dict:
                end_date = info_dict.get("end_date")
                end_time = datetime.datetime.combine(end_date.date(), end_time.time())
            else:  # else use start_time date
                end_time = datetime.datetime.combine(start_time.date(), end_time.time())
                if end_time < start_time:
                    end_time = end_time + datetime.timedelta(days=1)

    # Update info_dict
    info_dict["start_time"] = start_time
    info_dict["end_time"] = end_time

    # Remove unused fields
    info_dict.pop("start_date", None)
    info_dict.pop("end_date", None)
    return info_dict


def _get_info_from_filename(filename, filename_patterns):
    """Retrieve file information dictionary from filename."""
    if isinstance(filename_patterns, str):
        filename_patterns = [filename_patterns]
    valid_pattern_found = False
    for pattern in filename_patterns:
        try:
            info_dict = parse_filename_pattern(filename, pattern=pattern)
            if "start_time" in info_dict and "end_time" in info_dict:
                valid_pattern_found = True
        except Exception:
            pass
        if valid_pattern_found:
            break

    if not valid_pattern_found:
        return ValueError("Invalid pattern specified.")
    # Return info dictionary
    return info_dict


def get_info_from_filepath(filepath, filename_pattern):
    """Retrieve file information dictionary from filepath."""
    if not isinstance(filepath, str):
        raise TypeError("'filepath' must be a string.")
    filename = os.path.basename(filepath)
    return _get_info_from_filename(filename, filename_patterns=filename_pattern)


def get_key_from_filepath(filepath, key, filename_pattern):
    """Extract specific key information from a list of filepaths."""
    return get_info_from_filepath(filepath, filename_pattern=filename_pattern)[key]


def get_key_from_filepaths(filepaths, key, filename_pattern):
    """Extract specific key information from a list of filepaths."""
    if isinstance(filepaths, str):
        filepaths = [filepaths]
    return [get_key_from_filepath(filepath, key=key, filename_pattern=filename_pattern) for filepath in filepaths]


def get_start_time_from_filepaths(filepaths, filename_pattern):
    """Infer granules ``start_time`` from file paths."""
    return get_key_from_filepaths(filepaths, key="start_time", filename_pattern=filename_pattern)


def get_start_end_time_from_filepaths(filepaths, filename_pattern):
    """Infer granules ``start_time`` and ``end_time`` from file paths."""
    list_start_time = get_key_from_filepaths(filepaths, key="start_time", filename_pattern=filename_pattern)
    list_end_time = get_key_from_filepaths(filepaths, key="end_time", filename_pattern=filename_pattern)
    return np.array(list_start_time), np.array(list_end_time)
