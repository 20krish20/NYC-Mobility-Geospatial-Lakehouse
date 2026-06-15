# Real-Time NYC Mobility Lakehouse

A real-time + batch geospatial data lakehouse built on **Citi Bike** data,
demonstrating the core stack used in modern geospatial / transportation data
engineering roles: **Kafka, Spark Structured Streaming, Apache Sedona, H3
spatial indexing, Delta Lake medallion architecture, PostGIS, and FastAPI.**

## Problem statement

Bike-share operators need to know, in near real time, **which stations are
about to run empty (no bikes) or full (no docks)** so rebalancing trucks can
be dispatched proactively. This project ingests the live Citi Bike GBFS feed,
enriches it with spatial context (H3 cell, borough), detects sustained
imbalance "hotspots", and serves the results via an API and map dashboard —
the same pattern used for demand-hotspot detection in any fleet/mobility
operation.

## Architecture

```
GBFS poller (Python) --> Kafka topic "station-status"
        |
        v
Spark Structured Streaming consumer
  - join with station_information (broadcast)
  - compute H3 index (res 9) per station
  - Sedona point-in-polygon join -> borough
  - compute imbalance_ratio = bikes_available / capacity
  - flag sustained hotspots
        |
        v
Delta Lake (medallion architecture)
  bronze:     raw station_status events
  quarantine: records failing data-quality checks (bad coords, capacity <= 0, nulls)
  silver:     enriched events (+ h3_index, borough, imbalance_ratio)
  gold:       rolling hotspot aggregates per H3 cell / borough / window

Snapshot writer (standalone, polls silver every 60s):
  latest row per station_id --> upsert into Postgres "stations" table

Serving layer:
  Postgres + PostGIS (latest snapshot + borough polygons)
  FastAPI: /stations/nearby, /hotspots, /h3/{res}/cells, /health

Dashboard:
  Streamlit + pydeck map of stations (colored by imbalance_ratio) &
  H3 hexagon hotspot heatmap, pulling from the FastAPI endpoints
```

A second batch pipeline (Spark + Sedona) ingests historical Citi Bike trip
CSVs, H3-bins origin/destination coordinates, spatially joins them to NYC
borough polygons, and writes a `gold_trips` Delta table for demand/flow
analysis between H3 cells and boroughs.

## Technology map

| Technology   | Where it's used |
|--------------|------------------|
| **Kafka**        | `src/ingestion/gbfs_poller.py` publishes live station status |
| **Spark Structured Streaming** | `src/streaming/station_status_stream.py` consumes and enriches |
| **Apache Sedona** | `src/streaming/spatial_join.py`, `src/batch/load_historical_trips.py` — point-in-polygon joins |
| **H3**            | `src/streaming/h3_utils.py` — spatial indexing of stations and trip endpoints |
| **Delta Lake**    | bronze/silver/gold/quarantine tables for streaming and batch pipelines |
| **Data quality**  | `src/data_quality/expectations.py` — bronze->silver checks (NYC bbox, capacity, nulls), failing records routed to a quarantine table |
| **PostGIS**       | `db/init/`, `src/serving/api/db.py` — station snapshot + borough polygons, nearest-station queries |
| **FastAPI**       | `src/serving/api/` — `/stations/nearby`, `/hotspots`, `/h3/{res}/cells`, `/health` |
| **Streamlit**     | `src/serving/dashboard/app.py` — live map + hotspot heatmap + borough summary |
| **Snapshot writer** | `src/serving/snapshot_writer.py` — silver Delta -> Postgres `stations` table, the data source for `/stations/nearby` |

## Repo layout

```
src/
  ingestion/    # GBFS poller -> Kafka
  streaming/    # Spark Structured Streaming + H3 + Sedona enrichment
  batch/        # historical trip batch pipeline
  serving/      # FastAPI + Streamlit
  data_quality/ # bronze->silver validation checks
data/
  geo/          # NYC borough boundaries (GeoJSON, committed)
  sample/       # small fixtures used by the test suite
db/init/        # PostGIS schema + borough polygon load, run on container init
tests/
```

## Quickstart

```bash
docker compose up
```

This brings up Postgres+PostGIS (pre-loaded with NYC borough polygons), a
single-broker Kafka, the GBFS poller (publishing live station status to the
`station-status` topic every 60 seconds), the FastAPI serving layer at
http://localhost:8000 (docs at `/docs`), and the Streamlit dashboard at
http://localhost:8501.

The Spark jobs (streaming enrichment, historical batch, snapshot writer) are
run separately on the host -- they need a JVM + Sedona/Delta jars and aren't
containerized to keep the Compose stack lightweight:

```bash
python -m streaming.station_status_stream   # bronze/silver/gold Delta tables
python -m serving.snapshot_writer            # silver -> Postgres "stations" table
```

Without these running, `/hotspots` and `/h3/{res}/cells` return empty lists
and `/stations/nearby` returns no rows -- `/health` reports both states.

## Development

```bash
pip install -e ".[dev]"
pytest --cov=src
ruff check .
black --check .
```

All tests run against committed fixtures in `data/sample/` — no network or
live GBFS access required.

The streaming/spatial tests spin up a local Spark session with Sedona and
Delta Lake. On first run, Spark/Ivy downloads the Sedona, GeoTools, and Delta
jars from Maven Central (cached afterwards in `~/.ivy2`) — this requires
Java 17 and a one-time internet connection, but no GBFS/Kafka access.

The Postgres-backed API and snapshot-writer tests (`tests/test_api.py`,
`tests/test_snapshot_writer.py`) need a running Postgres+PostGIS with the
schema from `db/init/01_schema.sql` -- run `docker compose up -d postgres`
first. If Postgres isn't reachable, those tests are skipped automatically.

## Status

This project is being built incrementally, phase by phase:

- [x] **Phase 1** — Project scaffold, Docker Compose (Postgres/PostGIS + Kafka),
      NYC borough polygons loaded into PostGIS, GBFS poller with retry/backoff
      publishing to Kafka
- [x] **Phase 2** — Spark Structured Streaming enrichment (H3 + Sedona spatial
      join), bronze/silver Delta tables
- [x] **Phase 3** — Hotspot detection + gold aggregates
- [x] **Phase 4** — Historical batch trip pipeline + data quality checks
- [x] **Phase 5** — FastAPI serving layer (`/stations/nearby`, `/hotspots`,
      `/h3/{res}/cells`, `/health`) + Streamlit dashboard + standalone
      station snapshot writer (silver Delta -> Postgres)
- [ ] **Phase 6 (stretch)** — NYC TLC large-scale batch + Iceberg
