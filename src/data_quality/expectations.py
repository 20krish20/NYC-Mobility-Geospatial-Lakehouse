"""Bronze -> silver data quality checks for the station_status pipeline.

Implemented as small, named PySpark column-predicate checks rather than
Great Expectations or pandera: both add non-trivial setup and version-pinning
overhead on top of the existing Spark/Sedona/Delta stack, while a handful of
boolean expectations cover everything this pipeline needs and are trivial to
unit test against synthetic DataFrames.

Records that fail one or more checks are routed to a quarantine table (see
``apply_expectations``) tagged with a ``_dq_failures`` column listing every
failed check name, rather than being silently dropped or written to silver.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Rough NYC bounding box (covers all five boroughs with margin).
NYC_LAT_RANGE = (40.4, 41.0)
NYC_LON_RANGE = (-74.3, -73.65)

REQUIRED_FIELDS = ["station_id", "lat", "lon", "capacity", "h3_index", "borough"]


@dataclass(frozen=True)
class Expectation:
    """A named boolean predicate; ``condition`` is True when a row passes."""

    name: str
    condition: Column


def required_fields_not_null(fields: list[str] = REQUIRED_FIELDS) -> list[Expectation]:
    return [
        Expectation(name=f"{field}_not_null", condition=F.col(field).isNotNull())
        for field in fields
    ]


def lat_lon_within_nyc_bbox() -> list[Expectation]:
    lat_min, lat_max = NYC_LAT_RANGE
    lon_min, lon_max = NYC_LON_RANGE
    return [
        Expectation(name="lat_within_nyc_bbox", condition=F.col("lat").between(lat_min, lat_max)),
        Expectation(name="lon_within_nyc_bbox", condition=F.col("lon").between(lon_min, lon_max)),
    ]


def capacity_positive() -> list[Expectation]:
    return [Expectation(name="capacity_positive", condition=F.col("capacity") > 0)]


def silver_expectations() -> list[Expectation]:
    """The full set of bronze -> silver checks applied by ``apply_expectations``."""
    return [*required_fields_not_null(), *lat_lon_within_nyc_bbox(), *capacity_positive()]


def apply_expectations(
    df: DataFrame, expectations: list[Expectation] | None = None
) -> tuple[DataFrame, DataFrame]:
    """Split ``df`` into (valid, quarantined) DataFrames based on ``expectations``.

    A row is valid only if every expectation's condition holds. Quarantined
    rows retain a ``_dq_failures`` array column naming every failed check;
    valid rows are returned with the original schema, unchanged.
    """
    expectations = expectations if expectations is not None else silver_expectations()

    failure_names = F.array(*[F.when(~exp.condition, F.lit(exp.name)) for exp in expectations])
    with_failures = df.withColumn("_dq_failures", F.filter(failure_names, lambda x: x.isNotNull()))

    valid = with_failures.filter(F.size("_dq_failures") == 0).drop("_dq_failures")
    quarantined = with_failures.filter(F.size("_dq_failures") > 0)

    return valid, quarantined
