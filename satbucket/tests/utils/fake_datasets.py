import numpy as np
import pandas as pd
import pyproj
import xarray as xr
from gpm.dataset.crs import set_dataset_crs


def get_geodesic_path(
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
    n_points: int,
    offset_distance: float = 0,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute geodesic path between starting and ending coordinates."""
    geod = pyproj.Geod(ellps="sphere")
    r = geod.inv_intermediate(
        start_lon,
        start_lat,
        end_lon,
        end_lat,
        n_points,
        initial_idx=0,
        terminus_idx=0,
        return_back_azimuth=False,
        flags=pyproj.enums.GeodIntermediateFlag.AZIS_KEEP,
    )

    orthogonal_directions = np.array(r.azis) + 90

    if offset_distance != 0:
        geod.fwd(r.lons, r.lats, orthogonal_directions, [offset_distance] * n_points, inplace=True)

    # Convert into numpy arrays
    lon = np.array(r.lons)
    lat = np.array(r.lats)

    return lon, lat


def get_geodesic_band(
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
    width: float,
    n_along_track: int,
    n_cross_track: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute coordinates of geodesic band."""
    lon_lines = []
    lat_lines = []
    offsets = np.linspace(-width / 2, width / 2, n_cross_track)

    for offset in offsets:
        lon_line, lat_line = get_geodesic_path(
            start_lon=start_lon,
            start_lat=start_lat,
            end_lon=end_lon,
            end_lat=end_lat,
            n_points=n_along_track,
            offset_distance=offset,
        )
        lon_lines.append(lon_line)
        lat_lines.append(lat_line)

    lon = np.stack(lon_lines)
    lat = np.stack(lat_lines)

    return lon, lat


def get_orbit_dataarray(
    start_lon: float,
    start_lat: float,
    end_lon: float,
    end_lat: float,
    width: float,
    n_along_track: int,
    n_cross_track: int,
    n_range=0,
) -> xr.DataArray:
    """Create orbit data array on geodesic band."""
    rng = np.random.default_rng(seed=0)
    data = rng.random((n_cross_track, n_along_track))
    granule_id = np.zeros(n_along_track, dtype=int)
    cross_track_id = np.arange(0, n_cross_track)
    along_track_id = np.arange(0, n_along_track)
    gpm_id = [str(g) + "-" + str(z) for g, z in zip(granule_id, along_track_id, strict=False)]
    timesteps = pd.date_range("2000-01-01", periods=n_along_track, freq="s")

    # Coordinates
    lon, lat = get_geodesic_band(
        start_lon=start_lon,
        start_lat=start_lat,
        end_lon=end_lon,
        end_lat=end_lat,
        width=width,
        n_along_track=n_along_track,
        n_cross_track=n_cross_track,
    )
    # Create data array
    da = xr.DataArray(data, dims=["cross_track", "along_track"])
    da.coords["lat"] = (("cross_track", "along_track"), lat)
    da.coords["lon"] = (("cross_track", "along_track"), lon)
    da.coords["gpm_granule_id"] = ("along_track", granule_id)
    da.coords["gpm_cross_track_id"] = ("cross_track", cross_track_id)
    da.coords["gpm_along_track_id"] = ("along_track", along_track_id)
    da.coords["gpm_id"] = ("along_track", gpm_id)
    da.coords["time"] = ("along_track", timesteps)

    # Add range dimension if n_range not zero
    if n_range != 0:
        da = da.expand_dims(dim={"range": n_range})
        # - Add range coordinate
        da.coords["range"] = ("range", np.arange(1, n_range + 1))
        da.coords["gpm_range_id"] = ("range", np.arange(0, n_range))
        # - Add height
        height1d = xr.DataArray(np.arange(n_range)[::-1] * 1000, dims="range")
        height_3d = xr.ones_like(da) * height1d
        da = da.assign_coords({"height": height_3d})

    # Add CRS
    crs = pyproj.CRS(proj="longlat", ellps="WGS84")
    da = set_dataset_crs(da, crs=crs)
    return da
