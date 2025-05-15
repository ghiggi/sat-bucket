import datetime

import pytest

from satbucket.info import parse_filename_pattern


class TestParseFilenamePattern:

    def test_full_datetime_start_and_end(self):
        """Test parsing when both start_time and end_time are full datetime objects."""
        filename_pattern = "{start_time:%Y%m%dT%H%M%S}-{end_time:%Y%m%dT%H%M%S}"
        filename = "20240501T120000-20240501T123000"
        result = parse_filename_pattern(filename, filename_pattern)

        assert result["start_time"] == datetime.datetime(2024, 5, 1, 12, 0, 0)
        assert result["end_time"] == datetime.datetime(2024, 5, 1, 12, 30, 0)

    def test_modis_l1b_granule(self):
        """Test parsing MODIS L1B granule."""
        filename_pattern = "{product:s}.A{start_time:%Y%j.%H%M}.{others:s}.{processing_time:s}.{data_format}"
        filename = "MOD021KM.A2018358.1010.061.2018358192717.hdf"
        result = parse_filename_pattern(filename, filename_pattern)
        assert result["start_time"] == datetime.datetime(2018, 12, 24, 10, 10)
        assert result["end_time"] == datetime.datetime(2018, 12, 24, 12, 10)

    def test_gpm_rs_granule(self):
        """Test parsing when both start_time and end_time are full datetime objects."""
        filename_pattern = "{product_level:s}.{satellite:s}.{sensor:s}.{algorithm:s}.{start_date:%Y%m%d}-S{start_time:%H%M%S}-E{end_time:%H%M%S}.{granule_id}.{version}.{data_format}"  # noqa
        filename = "2A.GPM.DPR.V9-20211125.20210705-S013942-E031214.041760.V07A.HDF5"
        result = parse_filename_pattern(filename, filename_pattern)
        assert result["start_time"] == datetime.datetime(2021, 7, 5, 1, 39, 42)
        assert result["end_time"] == datetime.datetime(2021, 7, 5, 3, 12, 14)

        filename_pattern = "{product_level:s}.{satellite:s}.{sensor:s}.{algorithm:s}.{start_time:%Y%m%d-S%H%M%S}-E{end_time:%H%M%S}.{granule_id}.{version}.{data_format}"  # noqa
        filename = "2A.GPM.DPR.V9-20211125.20210705-S013942-E031214.041760.V07A.HDF5"
        result = parse_filename_pattern(filename, filename_pattern)
        assert result["start_time"] == datetime.datetime(2021, 7, 5, 1, 39, 42)
        assert result["end_time"] == datetime.datetime(2021, 7, 5, 3, 12, 14)

    def test_time_only_with_start_date(self):
        """Test parsing when times are time-only and combined with start_date."""
        filename_pattern = "{start_date:%Y%m%d}-S{start_time:%H%M%S}-E{end_time:%H%M%S}"
        filename = "20240501-S120000-E123000"
        result = parse_filename_pattern(filename, filename_pattern)

        assert result["start_time"] == datetime.datetime(2024, 5, 1, 12, 0, 0)
        assert result["end_time"] == datetime.datetime(2024, 5, 1, 12, 30, 0)

    def test_end_time_wraps_to_next_day(self):
        """Test that end_time wraps to the next day when earlier than start_time."""
        filename_pattern = "{start_date:%Y%m%d}-S{start_time:%H%M%S}-E{end_time:%H%M%S}"
        filename = "20240501-S230000-E003000"
        result = parse_filename_pattern(filename, filename_pattern)

        assert result["start_time"] == datetime.datetime(2024, 5, 1, 23, 0, 0)
        assert result["end_time"] == datetime.datetime(2024, 5, 2, 0, 30, 0)

    def test_time_only_with_start_and_end_date(self):
        """Test combining start/end times with explicitly provided dates."""
        filename_pattern = "{start_date:%Y%m%d}-S{start_time:%H%M%S}-{end_date:%Y%m%d}-E{end_time:%H%M%S}"
        filename = "20240501-S230000-20240502-E003000"
        result = parse_filename_pattern(filename, filename_pattern)

        assert result["start_time"] == datetime.datetime(2024, 5, 1, 23, 0, 0)
        assert result["end_time"] == datetime.datetime(2024, 5, 2, 0, 30, 0)

    def test_missing_start_date_raises_error(self):
        """Test that missing start_date raises an error when start_time is time-only."""
        filename_pattern = "S{start_time:%H%M%S}-E{end_time:%H%M%S}"
        filename = "S120000-E123000"
        with pytest.raises(ValueError, match="start_time is a time object but start_date is missing"):
            parse_filename_pattern(filename, filename_pattern)

    def test_invalid_end_time_raises_error(self):
        """Test that missing end_time raises an error."""
        filename_pattern = "{start_date:%Y%m%d}-S{start_time:%H%M%S}"
        filename = "20240501-S120000"
        result = parse_filename_pattern(filename, filename_pattern)
        assert result["start_time"] == datetime.datetime(2024, 5, 1, 12, 0, 0)
        assert result["end_time"] == datetime.datetime(2024, 5, 1, 12 + 2, 0, 0)
