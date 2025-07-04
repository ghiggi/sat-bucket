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
"""This directory defines the sat-bucket geographic binning toolbox."""
import contextlib
from importlib.metadata import PackageNotFoundError, version

from satbucket.partitioning import LonLatPartitioning, TilePartitioning, XYPartitioning
from satbucket.readers import read_bucket as read
from satbucket.routines import merge_granule_buckets, write_bucket, write_granules_bucket

__all__ = [
    "LonLatPartitioning",
    "TilePartitioning",
    "XYPartitioning",
    "merge_granule_buckets",
    "read",
    "write_bucket",
    "write_granules_bucket",
]


# Get version
with contextlib.suppress(PackageNotFoundError):
    __version__ = version("satbucket")
