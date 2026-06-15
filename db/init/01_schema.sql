-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- Borough boundary polygons (loaded from data/geo/nyc_boroughs.geojson)
CREATE TABLE IF NOT EXISTS boroughs (
    borocode SMALLINT PRIMARY KEY,
    boroname TEXT NOT NULL,
    geom     GEOMETRY(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS boroughs_geom_idx ON boroughs USING GIST (geom);

-- Latest snapshot of each Citi Bike station, kept up to date by the
-- streaming pipeline (silver layer writes through to this table).
CREATE TABLE IF NOT EXISTS stations (
    station_id           TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    lat                  DOUBLE PRECISION NOT NULL,
    lon                  DOUBLE PRECISION NOT NULL,
    capacity             INTEGER NOT NULL,
    geom                 GEOMETRY(Point, 4326) NOT NULL,
    h3_index             TEXT,
    borough              TEXT,
    num_bikes_available  INTEGER,
    num_docks_available  INTEGER,
    imbalance_ratio      DOUBLE PRECISION,
    last_reported        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS stations_geom_idx ON stations USING GIST (geom);
CREATE INDEX IF NOT EXISTS stations_h3_idx ON stations (h3_index);
CREATE INDEX IF NOT EXISTS stations_borough_idx ON stations (borough);
