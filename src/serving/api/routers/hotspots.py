from __future__ import annotations

from fastapi import APIRouter, Query, Request

from serving.api.db import latest_window_hotspots
from serving.api.models import HotspotOut

router = APIRouter(tags=["hotspots"])


@router.get("/hotspots", response_model=list[HotspotOut])
def get_hotspots(request: Request, borough: str | None = Query(None)) -> list[HotspotOut]:
    """Sustained empty/full hotspots from the most recent gold_hotspots window."""
    hotspots_df = request.app.state.gold_hotspots_reader()
    latest = latest_window_hotspots(hotspots_df)
    if borough is not None:
        latest = latest[latest["borough"] == borough]
    return [HotspotOut(**row) for row in latest.to_dict(orient="records")]
