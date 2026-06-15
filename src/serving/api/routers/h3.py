from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from serving.api.db import latest_window_hotspots
from serving.api.h3_aggregation import aggregate_h3_cells
from serving.api.models import H3CellOut

router = APIRouter(tags=["h3"])


@router.get("/h3/{resolution}/cells", response_model=list[H3CellOut])
def get_h3_cells(
    request: Request, resolution: int, borough: str | None = Query(None)
) -> list[H3CellOut]:
    """H3 hexagon hotspot aggregates at ``resolution``, for the most recent window."""
    hotspots_df = request.app.state.gold_hotspots_reader()
    latest = latest_window_hotspots(hotspots_df)
    if borough is not None:
        latest = latest[latest["borough"] == borough]

    try:
        cells = aggregate_h3_cells(latest, resolution)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return [H3CellOut(**row) for row in cells.to_dict(orient="records")]
