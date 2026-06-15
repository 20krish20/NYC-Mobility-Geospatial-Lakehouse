"""Spark Structured Streaming job: Kafka ``station-status`` -> bronze/silver Delta tables.

Pipeline:

1. Read raw station status events from Kafka (JSON) -> **bronze** Delta table
   (append-only, untouched).
2. Broadcast-join each event with the ``station_information`` reference data
   (written by the GBFS poller) to bring in name/lat/lon/capacity.
3. Enrich: H3 cell index (resolution 9), imbalance ratio, and borough via a
   Sedona point-in-polygon join against the NYC borough polygons.
4. Write the enriched stream -> **silver** Delta table.
5. Aggregate the enriched stream into 15-minute windows per H3 cell, flag
   sustained empty/full hotspots -> **gold** ``gold_hotspots`` Delta table.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from streaming.h3_utils import H3_RESOLUTION, with_h3_index
from streaming.hotspot_detection import compute_gold_hotspots
from streaming.spark_session import get_spark_session
from streaming.spatial_join import join_stations_to_boroughs, load_borough_polygons

# Subset of GBFS station_status.json fields we care about.
STATION_STATUS_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("num_bikes_available", IntegerType()),
        StructField("num_docks_available", IntegerType()),
        StructField("is_renting", IntegerType()),
        StructField("is_returning", IntegerType()),
        StructField("last_reported", LongType()),
    ]
)

# station_information reference data, written by the GBFS poller.
STATION_INFO_SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("name", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("capacity", IntegerType()),
    ]
)


@dataclass(frozen=True)
class StreamConfig:
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "station-status"
    station_information_path: str = "data/reference/station_information.json"
    boroughs_geojson_path: str = "data/geo/nyc_boroughs.geojson"
    bronze_path: str = "data/lake/bronze/station_status"
    silver_path: str = "data/lake/silver/station_status"
    gold_path: str = "data/lake/gold/hotspots"
    checkpoint_dir: str = "data/checkpoints"

    @classmethod
    def from_env(cls, env: dict | None = None) -> StreamConfig:
        env = env if env is not None else os.environ
        return cls(
            kafka_bootstrap_servers=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic=env.get("KAFKA_TOPIC", "station-status"),
            station_information_path=env.get(
                "STATION_INFORMATION_PATH", "data/reference/station_information.json"
            ),
            boroughs_geojson_path=env.get("BOROUGHS_GEOJSON_PATH", "data/geo/nyc_boroughs.geojson"),
            bronze_path=env.get("BRONZE_PATH", "data/lake/bronze/station_status"),
            silver_path=env.get("SILVER_PATH", "data/lake/silver/station_status"),
            gold_path=env.get("GOLD_PATH", "data/lake/gold/hotspots"),
            checkpoint_dir=env.get("CHECKPOINT_DIR", "data/checkpoints"),
        )

    @property
    def bronze_checkpoint(self) -> str:
        return f"{self.checkpoint_dir}/bronze_station_status"

    @property
    def silver_checkpoint(self) -> str:
        return f"{self.checkpoint_dir}/silver_station_status"

    @property
    def gold_checkpoint(self) -> str:
        return f"{self.checkpoint_dir}/gold_hotspots"


def read_station_status_stream(spark, config: StreamConfig) -> DataFrame:
    """Read and parse the raw station-status Kafka topic into a streaming DataFrame."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = F.from_json(F.col("value").cast("string"), STATION_STATUS_SCHEMA).alias("data")
    return raw.select(parsed, F.col("timestamp").alias("kafka_timestamp")).select(
        "data.*", "kafka_timestamp"
    )


def load_station_information(spark, path: str) -> DataFrame:
    """Load the station_information reference file written by the GBFS poller.

    The reference file is a single pretty-printed JSON array (see
    ``ingestion.gbfs_poller.write_station_information``), hence ``multiLine``.
    """
    return spark.read.schema(STATION_INFO_SCHEMA).option("multiLine", "true").json(path)


def compute_imbalance_ratio(
    df: DataFrame,
    bikes_col: str = "num_bikes_available",
    capacity_col: str = "capacity",
    out_col: str = "imbalance_ratio",
) -> DataFrame:
    """Add ``imbalance_ratio = bikes_available / capacity``.

    Null when capacity is missing or zero (a station with zero docks cannot
    be meaningfully scored for imbalance).
    """
    ratio = F.when(
        F.col(capacity_col).isNotNull() & (F.col(capacity_col) > 0),
        F.col(bikes_col) / F.col(capacity_col),
    )
    return df.withColumn(out_col, ratio)


def enrich_station_status(
    status_df: DataFrame, station_info_df: DataFrame, boroughs_df: DataFrame
) -> DataFrame:
    """Join status events with station info and add H3 index, imbalance ratio, and borough."""
    enriched = status_df.join(F.broadcast(station_info_df), on="station_id", how="inner")
    enriched = with_h3_index(enriched, resolution=H3_RESOLUTION)
    enriched = compute_imbalance_ratio(enriched)
    enriched = join_stations_to_boroughs(enriched, boroughs_df)
    return enriched


def write_bronze_stream(stream_df: DataFrame, config: StreamConfig) -> StreamingQuery:
    return (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", config.bronze_checkpoint)
        .outputMode("append")
        .start(config.bronze_path)
    )


def write_silver_stream(stream_df: DataFrame, config: StreamConfig) -> StreamingQuery:
    return (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", config.silver_checkpoint)
        .outputMode("append")
        .start(config.silver_path)
    )


def write_gold_hotspots_stream(stream_df: DataFrame, config: StreamConfig) -> StreamingQuery:
    return (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", config.gold_checkpoint)
        .outputMode("append")
        .start(config.gold_path)
    )


def run(config: StreamConfig | None = None) -> None:
    config = config or StreamConfig.from_env()
    spark = get_spark_session(app_name="nyc-mobility-station-status-stream")

    raw_stream = read_station_status_stream(spark, config)
    write_bronze_stream(raw_stream, config)

    station_info_df = load_station_information(spark, config.station_information_path)
    boroughs_df = load_borough_polygons(spark, config.boroughs_geojson_path)
    enriched_stream = enrich_station_status(raw_stream, station_info_df, boroughs_df)
    write_silver_stream(enriched_stream, config)

    gold_stream = compute_gold_hotspots(enriched_stream)
    write_gold_hotspots_stream(gold_stream, config)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
