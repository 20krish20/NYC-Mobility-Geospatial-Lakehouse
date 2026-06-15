"""Hotspot detection: windowed imbalance-ratio aggregation per H3 cell -> gold_hotspots.

A cell is flagged a *sustained* hotspot only if every reading within the
window stayed below ``EMPTY_RATIO_THRESHOLD`` (persistently empty -> high
demand for bikes) or above ``FULL_RATIO_THRESHOLD`` (persistently full -> no
docks available for returns). A cell that merely dips below/above the
threshold briefly is classified ``normal``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

EMPTY_RATIO_THRESHOLD = 0.1
FULL_RATIO_THRESHOLD = 0.9

DEFAULT_WINDOW_DURATION = "15 minutes"
DEFAULT_WATERMARK_DELAY = "5 minutes"

STATUS_EMPTY = "empty"
STATUS_FULL = "full"
STATUS_NORMAL = "normal"


def with_event_time(
    df: DataFrame, source_col: str = "last_reported", out_col: str = "event_time"
) -> DataFrame:
    """Convert a GBFS epoch-seconds column into a timestamp usable for windowing."""
    return df.withColumn(out_col, F.to_timestamp(F.from_unixtime(F.col(source_col))))


def aggregate_hotspot_windows(
    enriched_df: DataFrame,
    event_time_col: str = "event_time",
    window_duration: str = DEFAULT_WINDOW_DURATION,
    watermark_delay: str = DEFAULT_WATERMARK_DELAY,
) -> DataFrame:
    """Compute rolling-window imbalance-ratio stats per H3 cell.

    Returns one row per (h3_index, borough, window) with the average,
    minimum, maximum imbalance ratio and number of readings observed.
    """
    return (
        enriched_df.withWatermark(event_time_col, watermark_delay)
        .groupBy(
            F.window(F.col(event_time_col), window_duration),
            F.col("h3_index"),
            F.col("borough"),
        )
        .agg(
            F.avg("imbalance_ratio").alias("avg_ratio"),
            F.min("imbalance_ratio").alias("min_ratio"),
            F.max("imbalance_ratio").alias("max_ratio"),
            F.count(F.lit(1)).alias("num_readings"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "h3_index",
            "borough",
            "avg_ratio",
            "min_ratio",
            "max_ratio",
            "num_readings",
        )
    )


def classify_hotspots(windowed_df: DataFrame) -> DataFrame:
    """Add a ``status`` column: ``empty``, ``full``, or ``normal``."""
    status = (
        F.when(F.col("max_ratio") < EMPTY_RATIO_THRESHOLD, F.lit(STATUS_EMPTY))
        .when(F.col("min_ratio") > FULL_RATIO_THRESHOLD, F.lit(STATUS_FULL))
        .otherwise(F.lit(STATUS_NORMAL))
    )
    return windowed_df.withColumn("status", status)


def compute_gold_hotspots(
    enriched_df: DataFrame,
    window_duration: str = DEFAULT_WINDOW_DURATION,
    watermark_delay: str = DEFAULT_WATERMARK_DELAY,
) -> DataFrame:
    """End-to-end gold_hotspots transform: event time -> windowed aggregation -> classification.

    ``enriched_df`` must contain ``h3_index``, ``borough``, ``imbalance_ratio``,
    and ``last_reported`` (epoch seconds), as produced by
    ``station_status_stream.enrich_station_status``.
    """
    with_time = with_event_time(enriched_df)
    windowed = aggregate_hotspot_windows(with_time, "event_time", window_duration, watermark_delay)
    return classify_hotspots(windowed)
