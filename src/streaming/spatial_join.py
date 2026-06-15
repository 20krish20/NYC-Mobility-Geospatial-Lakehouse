"""Sedona-based point-in-polygon spatial join: stations -> NYC boroughs."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from sedona.sql.st_constructors import ST_GeomFromGeoJSON, ST_Point
from sedona.sql.st_predicates import ST_Contains


def load_borough_polygons(spark: SparkSession, geojson_path: str) -> DataFrame:
    """Load the NYC borough boundaries GeoJSON into a DataFrame with a Sedona geometry column.

    Returns a DataFrame with columns: ``borocode``, ``boroname``, ``geom``.
    """
    raw = spark.read.option("multiline", "true").json(geojson_path)
    features = raw.select(F.explode("features").alias("feature"))
    return features.select(
        F.col("feature.properties.borocode").cast("int").alias("borocode"),
        F.col("feature.properties.boroname").alias("boroname"),
        ST_GeomFromGeoJSON(F.to_json(F.col("feature.geometry"))).alias("geom"),
    )


def join_stations_to_boroughs(
    stations_df: DataFrame,
    boroughs_df: DataFrame,
    lat_col: str = "lat",
    lon_col: str = "lon",
    out_col: str = "borough",
) -> DataFrame:
    """Left-join each station point to the borough polygon containing it.

    Stations whose point does not fall within any borough polygon (e.g. bad
    coordinates) get a null ``out_col``. Adds exactly one new column
    (``out_col``) to ``stations_df``.
    """
    point_col = ST_Point(stations_df[lon_col], stations_df[lat_col])
    stations_with_point = stations_df.withColumn("_geom", point_col)

    joined = stations_with_point.join(
        boroughs_df,
        on=ST_Contains(boroughs_df["geom"], stations_with_point["_geom"]),
        how="left",
    )

    return joined.withColumn(out_col, F.col("boroname")).drop(
        "_geom", "geom", "boroname", "borocode"
    )
