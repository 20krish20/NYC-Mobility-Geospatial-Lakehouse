"""H3 spatial indexing helpers.

Stations are indexed into H3 cells at a fixed resolution so that the
streaming and gold layers can group nearby stations together (e.g. for
hotspot aggregation) without an expensive spatial join.
"""

from __future__ import annotations

import h3
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Resolution 9 cells are ~0.1 km^2, roughly city-block sized -- fine grained
# enough to distinguish individual Citi Bike stations without exploding the
# number of distinct cells.
H3_RESOLUTION = 9


def lat_lon_to_h3(
    lat: float | None, lon: float | None, resolution: int = H3_RESOLUTION
) -> str | None:
    """Return the H3 cell index containing (lat, lon) at the given resolution.

    Returns None if either coordinate is missing.
    """
    if lat is None or lon is None:
        return None
    return h3.latlng_to_cell(lat, lon, resolution)


def h3_udf(resolution: int = H3_RESOLUTION):
    """Build a Spark UDF that computes the H3 cell index for (lat, lon) columns.

    The computation is inlined (rather than calling ``lat_lon_to_h3``) so the
    UDF closure only depends on the ``h3`` package, which is importable in
    Spark's Python workers regardless of how this project's own ``src``
    package is laid out on the driver.
    """

    def _compute(lat: float | None, lon: float | None) -> str | None:
        if lat is None or lon is None:
            return None
        return h3.latlng_to_cell(lat, lon, resolution)

    return udf(_compute, StringType())


def with_h3_index(
    df: DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
    out_col: str = "h3_index",
    resolution: int = H3_RESOLUTION,
) -> DataFrame:
    """Add an H3 cell index column computed from the given lat/lon columns."""
    indexer: Column = h3_udf(resolution)(df[lat_col], df[lon_col])
    return df.withColumn(out_col, indexer)
