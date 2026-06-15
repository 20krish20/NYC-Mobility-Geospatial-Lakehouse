from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request

from serving.api.db import connection_from_pool, fetch_nearby_stations
from serving.api.models import StationOut

router = APIRouter(tags=["stations"])


def get_connection(request: Request):
    yield from connection_from_pool(request.app.state.pool)


@router.get("/stations/nearby", response_model=list[StationOut])
def get_nearby_stations(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radius_m: float = Query(500, gt=0, le=50_000),
    limit: int = Query(50, gt=0, le=500),
    conn=Depends(get_connection),
) -> list[StationOut]:
    """Stations within ``radius_m`` meters of (lat, lon), nearest first."""
    rows = fetch_nearby_stations(conn, lat=lat, lon=lon, radius_m=radius_m, limit=limit)
    return [StationOut(**row) for row in rows]
