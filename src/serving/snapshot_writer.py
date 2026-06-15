"""Write the latest per-station snapshot from the silver Delta table into Postgres.

The FastAPI serving layer answers ``/stations/nearby`` from a PostGIS
``stations`` table holding one row per station (its most recent reading).
This module bridges the streaming pipeline's silver Delta table (one row per
status *event*) and that table: it keeps only the most recent event per
``station_id`` and upserts it into Postgres.

Run as a standalone loop (``python -m serving.snapshot_writer``), polling the
silver table on an interval -- decoupled from the Spark Structured Streaming
job so the API's data source doesn't depend on a long-lived JDBC connection
from within Spark.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from streaming.spark_session import get_spark_session

logger = logging.getLogger(__name__)

SNAPSHOT_COLUMNS = [
    "station_id",
    "name",
    "lat",
    "lon",
    "capacity",
    "h3_index",
    "borough",
    "num_bikes_available",
    "num_docks_available",
    "imbalance_ratio",
    "last_reported",
]

UPSERT_SQL = """
INSERT INTO stations (
    station_id, name, lat, lon, capacity, geom, h3_index, borough,
    num_bikes_available, num_docks_available, imbalance_ratio, last_reported
) VALUES %s
ON CONFLICT (station_id) DO UPDATE SET
    name = EXCLUDED.name,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    capacity = EXCLUDED.capacity,
    geom = EXCLUDED.geom,
    h3_index = EXCLUDED.h3_index,
    borough = EXCLUDED.borough,
    num_bikes_available = EXCLUDED.num_bikes_available,
    num_docks_available = EXCLUDED.num_docks_available,
    imbalance_ratio = EXCLUDED.imbalance_ratio,
    last_reported = EXCLUDED.last_reported
"""

UPSERT_TEMPLATE = """(
    %(station_id)s, %(name)s, %(lat)s, %(lon)s, %(capacity)s,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
    %(h3_index)s, %(borough)s, %(num_bikes_available)s,
    %(num_docks_available)s, %(imbalance_ratio)s, to_timestamp(%(last_reported)s)
)"""

DEFAULT_POSTGRES_DSN = "postgresql://mobility:mobility@localhost:5432/mobility"


@dataclass(frozen=True)
class SnapshotWriterConfig:
    silver_path: str = "data/lake/silver/station_status"
    postgres_dsn: str = DEFAULT_POSTGRES_DSN
    poll_interval_seconds: float = 60.0

    @classmethod
    def from_env(cls, env: dict | None = None) -> SnapshotWriterConfig:
        env = env if env is not None else os.environ
        return cls(
            silver_path=env.get("SILVER_PATH", "data/lake/silver/station_status"),
            postgres_dsn=env.get("POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
            poll_interval_seconds=float(env.get("SNAPSHOT_POLL_INTERVAL_SECONDS", 60.0)),
        )


def latest_station_snapshot(silver_df: DataFrame) -> DataFrame:
    """Reduce the silver event stream to one (most recent) row per station_id."""
    window = Window.partitionBy("station_id").orderBy(F.col("last_reported").desc())
    return (
        silver_df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(*SNAPSHOT_COLUMNS)
    )


def upsert_stations(conn, rows: list[dict]) -> int:
    """Upsert station snapshot rows into the Postgres ``stations`` table.

    Returns the number of rows upserted.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, template=UPSERT_TEMPLATE)
    conn.commit()
    return len(rows)


def run_once(spark, conn, config: SnapshotWriterConfig) -> int:
    """Read the silver table, compute the latest snapshot, and upsert it. Returns row count."""
    silver_df = spark.read.format("delta").load(config.silver_path)
    snapshot_df = latest_station_snapshot(silver_df)
    rows = [row.asDict() for row in snapshot_df.collect()]
    return upsert_stations(conn, rows)


def run(config: SnapshotWriterConfig | None = None) -> None:
    config = config or SnapshotWriterConfig.from_env()
    spark = get_spark_session(app_name="nyc-mobility-snapshot-writer")
    conn = psycopg2.connect(config.postgres_dsn)

    logger.info(
        "Starting station snapshot writer: %s -> %s every %.0fs",
        config.silver_path,
        config.postgres_dsn,
        config.poll_interval_seconds,
    )

    try:
        while True:
            try:
                count = run_once(spark, conn, config)
                logger.info("Upserted %d station snapshots", count)
            except Exception:
                logger.exception("Snapshot upsert cycle failed; will retry next interval")
            time.sleep(config.poll_interval_seconds)
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    run()
