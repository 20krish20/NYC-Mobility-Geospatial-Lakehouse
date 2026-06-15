# Real-Time NYC Mobility Lakehouse

## Context
Portfolio project for a data engineer (Python/PySpark/SQL/Kafka/Spark Streaming/
Delta Lake/AWS/Docker/FastAPI background, MS Data Science from UMD) targeting
geospatial/transportation data engineering roles. Demonstrates the recurring
geospatial DE stack: PostGIS, Apache Sedona (Spark spatial), H3 spatial indexing,
spatial joins, streaming geo-ingestion, Delta Lake medallion architecture,
hotspot/anomaly detection, FastAPI serving layer, map dashboard.

Build it as a real, runnable, well-tested repo — not a notebook demo. Prioritize
correctness, clean architecture, and test coverage over breadth of features.

## Why Citi Bike (not NYC taxi data)
NYC TLC trip data post-2017 dropped lat/lon for zone IDs (weak for point-level
spatial work). Citi Bike's GBFS feed gives live, point-level station data
(no auth, ~60s updates) PLUS historical trip data with real start/end station
coordinates — a real streaming source AND batch source from one ecosystem.
NYC TLC data is an optional Phase 6 extension for large-scale batch if time allows.

## Data Sources
- **Real-time**: Citi Bike GBFS feed — https://gbfs.citibikenyc.com/gbfs/gbfs.json
  - `station_information.json` (static-ish: station_id, lat, lon, capacity, name)
  - `station_status.json` (dynamic: num_bikes_available, num_docks_available,
    last_reported) — poll every 60s
- **Historical**: Citi Bike System Data (monthly CSV/parquet) —
  https://s3.amazonaws.com/tripdata/index.html
  - start/end station id, lat/lon, timestamps, ride duration
- **Geo reference**: NYC Borough Boundaries (GeoJSON) — NYC Open Data, used for
  point-in-polygon joins (station -> borough/neighborhood)

## Architecture (medallion lakehouse + serving layer)

```
GBFS poller (Python) --> Kafka topic "station-status"
        |
        v
Spark Structured Streaming consumer
  - join with station_information (broadcast)
  - compute H3 index (res 9) per station
  - Sedona point-in-polygon join -> borough/neighborhood
  - compute imbalance_ratio = bikes_available / capacity
  - flag hotspot if imbalance breaches threshold, sustained > N minutes
        |
        v
Delta Lake (local filesystem)
  bronze: raw station_status events (append-only)
  silver: enriched events (+ h3_index, borough, imbalance_ratio)
  gold:   rolling hotspot aggregates per H3 cell / borough / time window
```

Historical batch job (Spark + Sedona):
Citi Bike trip CSVs -> spatial join trips to boroughs/neighborhoods via
start/end lat-lon -> H3-bin origin/destination -> write to Delta (gold_trips)

Serving layer:
- Postgres + PostGIS holds latest station snapshot + borough polygons
- FastAPI exposes:
  - `GET /stations/nearby?lat=&lon=&radius_m=` (PostGIS ST_DWithin)
  - `GET /hotspots?borough=` (from gold Delta, latest window)
  - `GET /h3/{resolution}/cells` (H3 hexagon aggregates for map)
  - `GET /health`

Dashboard: Streamlit + pydeck/folium
- live map of stations colored by imbalance_ratio
- H3 hexagon heatmap of demand/hotspots
- borough-level summary panel

## Tech stack & version notes
- Python 3.11, PySpark 3.5.x, delta-spark 3.x, apache-sedona 1.6.x (verify
  Sedona/Spark compatibility matrix before pinning — Sedona trails Spark releases)
- h3-py for spatial indexing
- Kafka via `bitnami/kafka` docker image (single-broker, no zookeeper needed for
  recent versions) — keep it simple, this is a portfolio project not a prod cluster
- PostgreSQL 16 + PostGIS extension via `postgis/postgis` docker image
- FastAPI + Pydantic v2 + uvicorn
- Streamlit + pydeck (or folium/streamlit-folium) for the map
- pytest + pytest-cov for tests; ruff + black for lint/format
- Great Expectations (or pandera, whichever has less setup overhead for a Spark
  DataFrame) for bronze->silver data quality checks
- Docker Compose orchestrates: postgres, kafka, GBFS poller, Spark streaming job,
  FastAPI, Streamlit
- GitHub Actions CI: lint + pytest on Python 3.11/3.12, on every push

## Repo structure
```
README.md
docker-compose.yml
pyproject.toml
src/
  ingestion/
    gbfs_poller.py            # polls GBFS, publishes to Kafka
  streaming/
    station_status_stream.py  # Spark Structured Streaming job
    h3_utils.py
    spatial_join.py           # Sedona point-in-polygon helpers
    hotspot_detection.py
  batch/
    load_historical_trips.py  # Citi Bike CSV -> Delta gold_trips
  serving/
    api/
      main.py                  # FastAPI app
      routers/
      db.py                    # PostGIS connection layer
    dashboard/
      app.py                   # Streamlit
  data_quality/
    expectations.py
tests/
  test_h3_utils.py
  test_spatial_join.py
  test_hotspot_detection.py
  test_api.py
data/
  geo/      # NYC borough GeoJSON (small, committed)
  sample/   # small sample station_status/trips for tests
.github/workflows/ci.yml
```

## Phased implementation plan
Work through phases in order; each phase should leave the repo in a working,
tested state — commit after each.

### Phase 1: Foundations
- Project scaffold, pyproject.toml, docker-compose with postgres+postgis and kafka
- Download/commit NYC borough GeoJSON into data/geo/
- Load borough polygons into PostGIS on container init (init SQL script)
- GBFS poller: fetch station_information + station_status, publish status to
  Kafka topic `station-status` every 60s, with retry/backoff. Unit test the
  parsing logic with a saved sample JSON fixture.

### Phase 2: Streaming enrichment
- Spark Structured Streaming job reads from Kafka, joins with broadcast
  station_information, computes:
  - H3 index at resolution 9 (h3-py) per station lat/lon
  - imbalance_ratio = num_bikes_available / capacity (handle capacity=0)
  - Sedona point-in-polygon join against borough polygons -> borough name
- Write bronze (raw) and silver (enriched) to local Delta Lake tables
- Unit tests for h3_utils and spatial_join using fixed lat/lon test points with
  known expected H3 cells / boroughs

### Phase 3: Hotspot detection + gold layer
- Streaming aggregation: for each H3 cell, compute rolling window (e.g. 15 min)
  avg imbalance_ratio; flag "hotspot" if a cell is persistently <0.1 (empty, high
  demand) or >0.9 (full, no docks) for the whole window
- Write gold tables: gold_hotspots (h3_index, borough, window, status, avg_ratio)
- Unit tests for hotspot_detection logic with synthetic time-series fixtures

### Phase 4: Historical batch + data quality
- Batch job: ingest one month of Citi Bike historical trip CSVs, spatial-join
  start/end coordinates to boroughs via Sedona, H3-bin origin/destination, write
  to gold_trips Delta table
- Add Great Expectations/pandera checks on bronze->silver (lat/lon within NYC
  bounding box, capacity > 0, no nulls in required fields) — document what
  happens to records that fail (quarantine table)

### Phase 5: Serving layer + dashboard
- FastAPI: /stations/nearby (PostGIS ST_DWithin), /hotspots, /h3/{res}/cells,
  /health — Pydantic response models, integration tests with a test Postgres
  (testcontainers or docker-compose test profile)
- Streamlit dashboard: live station map colored by imbalance_ratio, H3 hexagon
  hotspot heatmap, borough summary stats — pull from FastAPI

### Phase 6 (stretch, optional): NYC TLC large-scale batch extension
- Second batch pipeline over NYC TLC trip data (zone-based) to demonstrate
  large-scale (multi-GB/Parquet) Spark processing + Iceberg table format as an
  alternative to Delta, for resumes that ask specifically for Iceberg

## CI/CD
- GitHub Actions: on push/PR, run ruff + black --check, then pytest with
  coverage, on Python 3.11 and 3.12 matrix
- All tests must run without requiring live GBFS/network access — use fixtures
  for everything

## README requirements (for resume/GitHub presentation)
- Architecture diagram (mermaid or image)
- Clear "what problem does this solve" framing: real-time bike-share rebalancing
  / demand hotspot detection — a genuine operational problem for mobility operators
- Quickstart: `docker compose up` brings up the full stack
- Explicitly call out which JD-relevant technologies are demonstrated where
  (PostGIS, Sedona, H3, Delta Lake, Kafka, FastAPI) so it's scannable

## Non-goals / constraints
- Must run fully locally via Docker Compose — no paid cloud services required
- Keep Kafka/Spark setup as lightweight as possible (single broker, local Spark
  master) — demonstration of correct architecture, not a production cluster
- Favor working, tested increments over a large untested initial commit
