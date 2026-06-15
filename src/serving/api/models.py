"""Pydantic response models for the FastAPI serving layer."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class StationOut(BaseModel):
    station_id: str
    name: str
    lat: float
    lon: float
    capacity: int
    h3_index: str | None = None
    borough: str | None = None
    num_bikes_available: int | None = None
    num_docks_available: int | None = None
    imbalance_ratio: float | None = None
    last_reported: datetime | None = None
    distance_m: float


class HotspotOut(BaseModel):
    h3_index: str
    borough: str | None = None
    window_start: datetime
    window_end: datetime
    avg_ratio: float
    min_ratio: float
    max_ratio: float
    num_readings: int
    status: str


class H3CellOut(BaseModel):
    h3_index: str
    resolution: int
    avg_ratio: float
    status: str
    num_readings: int


class HealthOut(BaseModel):
    status: str
    postgres: bool
    gold_hotspots_available: bool
