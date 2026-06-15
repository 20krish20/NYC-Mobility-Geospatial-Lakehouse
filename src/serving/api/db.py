"""Connections to the serving layer's two data sources: PostGIS and the gold Delta tables.

- **Postgres/PostGIS** holds the latest per-station snapshot (written by
  ``serving.snapshot_writer``) and the NYC borough polygons -- queried for
  ``/stations/nearby``.
- **Gold Delta tables** (``gold_hotspots``) are read directly with
  ``deltalake`` (no JVM/Spark session needed) for ``/hotspots`` and
  ``/h3/{resolution}/cells``.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable
from fastapi import HTTPException
from psycopg2.pool import SimpleConnectionPool

DEFAULT_POSTGRES_DSN = "postgresql://mobility:mobility@localhost:5432/mobility"

GOLD_HOTSPOTS_COLUMNS = [
    "window_start",
    "window_end",
    "h3_index",
    "borough",
    "avg_ratio",
    "min_ratio",
    "max_ratio",
    "num_readings",
    "status",
]


@dataclass(frozen=True)
class APIConfig:
    postgres_dsn: str = DEFAULT_POSTGRES_DSN
    gold_hotspots_path: str = "data/lake/gold/hotspots"

    @classmethod
    def from_env(cls, env: dict | None = None) -> APIConfig:
        env = env if env is not None else os.environ
        return cls(
            postgres_dsn=env.get("POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
            gold_hotspots_path=env.get("GOLD_HOTSPOTS_PATH", "data/lake/gold/hotspots"),
        )


def create_connection_pool(
    config: APIConfig, minconn: int = 1, maxconn: int = 5
) -> SimpleConnectionPool:
    return SimpleConnectionPool(minconn, maxconn, dsn=config.postgres_dsn)


def connection_from_pool(pool: SimpleConnectionPool | None) -> Iterator:
    """FastAPI dependency: borrow a connection from the pool for one request."""
    if pool is None:
        raise HTTPException(status_code=503, detail="database unavailable")
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


NEARBY_STATIONS_SQL = """
SELECT
    station_id,
    name,
    lat,
    lon,
    capacity,
    h3_index,
    borough,
    num_bikes_available,
    num_docks_available,
    imbalance_ratio,
    last_reported,
    ST_Distance(
        geom::geography,
        ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography
    ) AS distance_m
FROM stations
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography,
    %(radius_m)s
)
ORDER BY distance_m
LIMIT %(limit)s
"""


def fetch_nearby_stations(
    conn, lat: float, lon: float, radius_m: float, limit: int = 50
) -> list[dict]:
    """Return stations within ``radius_m`` meters of (lat, lon), nearest first."""
    with conn.cursor() as cur:
        cur.execute(
            NEARBY_STATIONS_SQL, {"lat": lat, "lon": lon, "radius_m": radius_m, "limit": limit}
        )
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def gold_hotspots_available(path: str) -> bool:
    """Whether the gold_hotspots Delta table has been written yet."""
    return (Path(path) / "_delta_log").exists()


def read_gold_hotspots(path: str) -> pd.DataFrame:
    """Read the gold_hotspots Delta table, or an empty frame if it doesn't exist yet."""
    if not gold_hotspots_available(path):
        return pd.DataFrame(columns=GOLD_HOTSPOTS_COLUMNS)
    return DeltaTable(path).to_pandas()


def latest_window_hotspots(hotspots_df: pd.DataFrame) -> pd.DataFrame:
    """Filter to the most recently closed aggregation window across all cells."""
    if hotspots_df.empty:
        return hotspots_df
    latest_window_end = hotspots_df["window_end"].max()
    return hotspots_df[hotspots_df["window_end"] == latest_window_end]
