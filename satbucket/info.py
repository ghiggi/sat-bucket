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
import os 
import datetime
from trollsift import Parser

 
def parse_filename_pattern(filename, filename_pattern):
    p = Parser(filename_pattern)
    info_dict = p.parse(filename)
    
    # Handle start_time
    start_time = info_dict.get("start_time")
    
    if isinstance(start_time, datetime.datetime):
        start_datetime = start_time
    elif isinstance(start_time, datetime.time):
        start_date = info_dict.get("start_date")
        if not isinstance(start_date, datetime.date):
            raise ValueError("start_time is a time object but start_date is missing or invalid.")
        start_datetime = datetime.datetime.combine(start_date, start_time)
    else:
        raise ValueError("Invalid or missing start_time in info_dict.")

    # Handle end_time
    end_time = info_dict.get("end_time")

    if isinstance(end_time, datetime.datetime):
        end_datetime = end_time
    elif isinstance(end_time, datetime.time):
        end_date = info_dict.get("end_date", None)
        if end_date and isinstance(end_date, datetime.date):
            end_datetime = datetime.datetime.combine(end_date, end_time)
        elif isinstance(start_datetime, datetime.datetime):
            end_datetime = start_datetime.replace(
                hour=end_time.hour,
                minute=end_time.minute,
                second=end_time.second
            )
            if end_datetime < start_datetime:
                end_datetime += datetime.timedelta(days=1)
        else:
            raise ValueError("Cannot resolve end_time: missing valid end_date or start_datetime.")
    else:
        raise ValueError("Invalid or missing end_time in info_dict.")
    
    # Update info_dict
    info_dict["start_time"] = start_datetime
    info_dict["end_time"] = end_datetime

    # Remove unused fields
    info_dict.pop("start_date", None)
    info_dict.pop("end_date", None)
    
    return info_dict

# def parse_filename_pattern(filename, pattern):
#     p = Parser(filename_pattern)
#     info_dict = p.parse(filename)
    
#     pattern = "{start_date:%Y%m%d}-S{start_time:%H%M%S}-E{end_time:%H%M%S}"
    
#     # Retrieve correct start_time and end_time
#     # If start_time does not contain date information, search also for start_date (otherwise raise error)
#     # Otherwise use start_time
    
#     # If end_time does not contain date information, search also for end_date. If end_date not present in info_dict, 
#     # use start_date and add + 1 day if necessary if end_datetime < start_datetime
#     # Otherwise use end_time
    
   
    
#     start_datetime = info_dict["start_date_time"]
#     end_time = info_dict["end_time"]
#     end_datetime = start_datetime.replace(
#         hour=end_time.hour,
#         minute=end_time.minute,
#         second=end_time.second,
#     )
#     if end_datetime < start_datetime:
#         end_datetime = end_datetime + datetime.timedelta(days=1)
#     info_dict.pop("start_date_time")
#     info_dict["start_time"] = start_datetime
#     info_dict["end_time"] = end_datetime
 

#     return info_dict


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