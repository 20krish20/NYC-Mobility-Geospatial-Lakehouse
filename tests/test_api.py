import os

import psycopg2
import pytest
from fastapi.testclient import TestClient

from serving.api.db import APIConfig
from serving.api.main import create_app
from serving.snapshot_writer import DEFAULT_POSTGRES_DSN
from streaming.hotspot_detection import compute_gold_hotspots

EMPIRE_STATE = (40.748817, -73.985428)
BARCLAYS = (40.682661, -73.975225)
WINDOW_START = 1704067200  # 2024-01-01T00:00:00Z, 15-min window boundary


def _postgres_dsn() -> str:
    return os.environ.get("TEST_POSTGRES_DSN", DEFAULT_POSTGRES_DSN)


def _postgres_available() -> bool:
    try:
        conn = psycopg2.connect(_postgres_dsn(), connect_timeout=2)
        conn.close()
        return True
    except psycopg2.OperationalError:
        return False


POSTGRES_AVAILABLE = _postgres_available()


@pytest.fixture
def gold_hotspots_path(spark, tmp_path):
    rows = [
        # Persistently empty cell near the Empire State Building.
        ("892a100d2d7ffff", "Manhattan", 0.05, WINDOW_START),
        ("892a100d2d7ffff", "Manhattan", 0.02, WINDOW_START + 300),
        ("892a100d2d7ffff", "Manhattan", 0.08, WINDOW_START + 600),
        # Normal cell near Barclays Center.
        ("892a100da77ffff", "Brooklyn", 0.5, WINDOW_START),
        ("892a100da77ffff", "Brooklyn", 0.6, WINDOW_START + 600),
    ]
    df = spark.createDataFrame(rows, ["h3_index", "borough", "imbalance_ratio", "last_reported"])
    gold_df = compute_gold_hotspots(df)

    path = str(tmp_path / "gold_hotspots")
    gold_df.write.format("delta").mode("overwrite").save(path)
    return path


@pytest.fixture
def app(gold_hotspots_path):
    config = APIConfig(postgres_dsn=_postgres_dsn(), gold_hotspots_path=gold_hotspots_path)
    return create_app(config)


@pytest.fixture
def client(app):
    with TestClient(app) as test_client:
        yield test_client


def test_hotspots_returns_latest_window_classified_cells(client):
    response = client.get("/hotspots")
    assert response.status_code == 200

    by_cell = {row["h3_index"]: row for row in response.json()}
    assert by_cell["892a100d2d7ffff"]["status"] == "empty"
    assert by_cell["892a100da77ffff"]["status"] == "normal"


def test_hotspots_filters_by_borough(client):
    response = client.get("/hotspots", params={"borough": "Manhattan"})
    assert response.status_code == 200

    boroughs = {row["borough"] for row in response.json()}
    assert boroughs == {"Manhattan"}


def test_h3_cells_at_native_resolution(client):
    response = client.get("/h3/9/cells")
    assert response.status_code == 200

    cells = {row["h3_index"]: row for row in response.json()}
    assert cells["892a100d2d7ffff"]["status"] == "empty"
    assert cells["892a100d2d7ffff"]["resolution"] == 9


def test_h3_cells_at_coarser_resolution_aggregates(client):
    response = client.get("/h3/7/cells")
    assert response.status_code == 200

    cells = response.json()
    assert all(row["resolution"] == 7 for row in cells)
    assert all(len(row["h3_index"]) == 15 for row in cells)


def test_h3_cells_rejects_finer_than_source_resolution(client):
    response = client.get("/h3/10/cells")
    assert response.status_code == 400


def test_health_reports_gold_hotspots_available(client):
    response = client.get("/health")
    assert response.status_code == 200

    body = response.json()
    assert body["gold_hotspots_available"] is True


@pytest.mark.skipif(not POSTGRES_AVAILABLE, reason="Postgres not reachable")
def test_health_reports_postgres_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["postgres"] is True


@pytest.mark.skipif(not POSTGRES_AVAILABLE, reason="Postgres not reachable")
def test_stations_nearby_returns_only_stations_within_radius(client):
    conn = psycopg2.connect(_postgres_dsn())
    rows = [
        ("test-near", "Near Station", EMPIRE_STATE[0] + 0.0005, EMPIRE_STATE[1], 20, "Manhattan"),
        ("test-far", "Far Station", BARCLAYS[0], BARCLAYS[1], 15, "Brooklyn"),
    ]
    try:
        with conn.cursor() as cur:
            for station_id, name, lat, lon, capacity, borough in rows:
                cur.execute(
                    """
                    INSERT INTO stations (station_id, name, lat, lon, capacity, geom, borough)
                    VALUES (%(station_id)s, %(name)s, %(lat)s, %(lon)s, %(capacity)s,
                            ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326), %(borough)s)
                    ON CONFLICT (station_id) DO UPDATE SET
                        lat = EXCLUDED.lat, lon = EXCLUDED.lon, geom = EXCLUDED.geom
                    """,
                    {
                        "station_id": station_id,
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "capacity": capacity,
                        "borough": borough,
                    },
                )
        conn.commit()

        response = client.get(
            "/stations/nearby",
            params={"lat": EMPIRE_STATE[0], "lon": EMPIRE_STATE[1], "radius_m": 1000},
        )
        assert response.status_code == 200

        station_ids = {row["station_id"] for row in response.json()}
        assert "test-near" in station_ids
        assert "test-far" not in station_ids
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM stations WHERE station_id IN ('test-near', 'test-far')")
        conn.commit()
        conn.close()
