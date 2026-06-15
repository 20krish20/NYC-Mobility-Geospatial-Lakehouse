import os

import psycopg2
import pytest

from serving.snapshot_writer import (
    DEFAULT_POSTGRES_DSN,
    SNAPSHOT_COLUMNS,
    latest_station_snapshot,
    upsert_stations,
)


def test_latest_station_snapshot_keeps_most_recent_per_station(spark):
    rows = [
        (
            "519",
            "W 21 St & 6 Ave",
            40.74173,
            -73.99416,
            39,
            "892a100d247ffff",
            "Manhattan",
            10,
            29,
            10 / 39,
            1700000000,
        ),
        (
            "519",
            "W 21 St & 6 Ave",
            40.74173,
            -73.99416,
            39,
            "892a100d247ffff",
            "Manhattan",
            12,
            27,
            12 / 39,
            1700000100,
        ),
        (
            "382",
            "Wythe Ave & Metropolitan Ave",
            40.71594678,
            -73.96334072,
            31,
            "892a100d337ffff",
            "Brooklyn",
            5,
            26,
            5 / 31,
            1700000050,
        ),
    ]
    df = spark.createDataFrame(rows, SNAPSHOT_COLUMNS)

    result = {row["station_id"]: row for row in latest_station_snapshot(df).collect()}

    assert result["519"]["num_bikes_available"] == 12
    assert result["519"]["last_reported"] == 1700000100
    assert result["382"]["num_bikes_available"] == 5
    assert set(result["519"].asDict().keys()) == set(SNAPSHOT_COLUMNS)


def _postgres_dsn() -> str:
    return os.environ.get("TEST_POSTGRES_DSN", DEFAULT_POSTGRES_DSN)


def _postgres_available() -> bool:
    try:
        conn = psycopg2.connect(_postgres_dsn(), connect_timeout=2)
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False


@pytest.mark.skipif(not _postgres_available(), reason="Postgres not reachable")
def test_upsert_stations_inserts_then_updates():
    conn = psycopg2.connect(_postgres_dsn())
    row = {
        "station_id": "test-snapshot-999",
        "name": "Test Station",
        "lat": 40.74173,
        "lon": -73.99416,
        "capacity": 10,
        "h3_index": "892a100d247ffff",
        "borough": "Manhattan",
        "num_bikes_available": 5,
        "num_docks_available": 5,
        "imbalance_ratio": 0.5,
        "last_reported": 1700000000,
    }
    try:
        upsert_stations(conn, [row])
        with conn.cursor() as cur:
            cur.execute(
                "SELECT num_bikes_available, borough FROM stations WHERE station_id = %s",
                (row["station_id"],),
            )
            bikes, borough = cur.fetchone()
            assert bikes == 5
            assert borough == "Manhattan"

        row["num_bikes_available"] = 8
        upsert_stations(conn, [row])
        with conn.cursor() as cur:
            cur.execute(
                "SELECT num_bikes_available FROM stations WHERE station_id = %s",
                (row["station_id"],),
            )
            assert cur.fetchone()[0] == 8
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stations WHERE station_id = %s", (row["station_id"],))
        conn.commit()
        conn.close()
