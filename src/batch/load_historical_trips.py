"""Historical batch pipeline: Citi Bike trip CSVs -> ``gold_trips`` Delta table.

For each trip, the start/end coordinates are H3-binned (resolution 9) and
spatially joined to NYC borough polygons via Sedona, producing one row per
trip with origin/destination H3 cell, origin/destination borough, and ride
duration -- suitable for demand/flow analysis between H3 cells or boroughs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from streaming.h3_utils import H3_RESOLUTION, with_h3_index
from streaming.spark_session import get_spark_session
from streaming.spatial_join import join_stations_to_boroughs, load_borough_polygons

# Citi Bike System Data trip CSV schema (2021+ format).
TRIP_SCHEMA = StructType(
    [
        StructField("ride_id", StringType()),
        StructField("rideable_type", StringType()),
        StructField("started_at", TimestampType()),
        StructField("ended_at", TimestampType()),
        StructField("start_station_name", StringType()),
        StructField("start_station_id", StringType()),
        StructField("end_station_name", StringType()),
        StructField("end_station_id", StringType()),
        StructField("start_lat", DoubleType()),
        StructField("start_lng", DoubleType()),
        StructField("end_lat", DoubleType()),
        StructField("end_lng", DoubleType()),
        StructField("member_casual", StringType()),
    ]
)


@dataclass(frozen=True)
class BatchConfig:
    trips_csv_path: str = "data/sample/citibike_trips_sample.csv"
    boroughs_geojson_path: str = "data/geo/nyc_boroughs.geojson"
    gold_trips_path: str = "data/lake/gold/trips"

    @classmethod
    def from_env(cls, env: dict | None = None) -> BatchConfig:
        env = env if env is not None else os.environ
        return cls(
            trips_csv_path=env.get("TRIPS_CSV_PATH", "data/sample/citibike_trips_sample.csv"),
            boroughs_geojson_path=env.get("BOROUGHS_GEOJSON_PATH", "data/geo/nyc_boroughs.geojson"),
            gold_trips_path=env.get("GOLD_TRIPS_PATH", "data/lake/gold/trips"),
        )


def read_trips(spark, path: str) -> DataFrame:
    """Read one or more Citi Bike trip CSVs (glob patterns supported)."""
    return spark.read.schema(TRIP_SCHEMA).option("header", "true").csv(path)


def with_trip_duration(df: DataFrame, out_col: str = "trip_duration_seconds") -> DataFrame:
    """Add ride duration in seconds, computed from ``started_at``/``ended_at``."""
    duration = F.col("ended_at").cast("long") - F.col("started_at").cast("long")
    return df.withColumn(out_col, duration)


def enrich_trips(trips_df: DataFrame, boroughs_df: DataFrame) -> DataFrame:
    """Add trip duration, origin/destination H3 cells, and origin/destination boroughs."""
    enriched = with_trip_duration(trips_df)
    enriched = with_h3_index(
        enriched,
        lat_col="start_lat",
        lon_col="start_lng",
        out_col="origin_h3",
        resolution=H3_RESOLUTION,
    )
    enriched = with_h3_index(
        enriched,
        lat_col="end_lat",
        lon_col="end_lng",
        out_col="destination_h3",
        resolution=H3_RESOLUTION,
    )
    enriched = join_stations_to_boroughs(
        enriched, boroughs_df, lat_col="start_lat", lon_col="start_lng", out_col="origin_borough"
    )
    enriched = join_stations_to_boroughs(
        enriched,
        boroughs_df,
        lat_col="end_lat",
        lon_col="end_lng",
        out_col="destination_borough",
    )
    return enriched


def write_gold_trips(df: DataFrame, config: BatchConfig) -> None:
    df.write.format("delta").mode("overwrite").save(config.gold_trips_path)


def run(config: BatchConfig | None = None) -> None:
    config = config or BatchConfig.from_env()
    spark = get_spark_session(app_name="nyc-mobility-historical-trips")

    trips_df = read_trips(spark, config.trips_csv_path)
    boroughs_df = load_borough_polygons(spark, config.boroughs_geojson_path)
    enriched = enrich_trips(trips_df, boroughs_df)

    write_gold_trips(enriched, config)


if __name__ == "__main__":
    run()
