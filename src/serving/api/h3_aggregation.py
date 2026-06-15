"""Re-aggregate gold_hotspots rows (computed at H3_RESOLUTION) to a coarser H3 resolution.

Used by ``/h3/{resolution}/cells`` so the map can show hexagon aggregates at
whatever zoom-appropriate resolution the frontend requests.
"""

from __future__ import annotations

import h3
import pandas as pd

from streaming.h3_config import H3_RESOLUTION

H3_CELL_COLUMNS = ["h3_index", "resolution", "avg_ratio", "status", "num_readings"]

STATUS_MIXED = "mixed"


def aggregate_h3_cells(hotspots_df: pd.DataFrame, resolution: int) -> pd.DataFrame:
    """Aggregate ``hotspots_df`` (one row per H3_RESOLUTION cell) to ``resolution``.

    ``resolution`` must be <= ``H3_RESOLUTION``: gold data is computed at a
    fixed resolution and cannot be disaggregated to a finer one.

    For each output cell, ``avg_ratio`` is the ``num_readings``-weighted
    average of its children's averages, ``num_readings`` is their sum, and
    ``status`` is the shared status if every child agrees, else ``"mixed"``.
    """
    if resolution > H3_RESOLUTION:
        raise ValueError(
            f"resolution must be <= {H3_RESOLUTION} (gold data is computed at "
            f"resolution {H3_RESOLUTION} and cannot be disaggregated to a finer one)"
        )

    if hotspots_df.empty:
        return pd.DataFrame(columns=H3_CELL_COLUMNS)

    if resolution == H3_RESOLUTION:
        out = hotspots_df[["h3_index", "avg_ratio", "status", "num_readings"]].copy()
        out["resolution"] = resolution
        return out[H3_CELL_COLUMNS].reset_index(drop=True)

    working = hotspots_df.copy()
    working["h3_index"] = working["h3_index"].apply(
        lambda cell: h3.cell_to_parent(cell, resolution)
    )

    def _combine(group: pd.DataFrame) -> pd.Series:
        total_readings = int(group["num_readings"].sum())
        weighted_avg = (group["avg_ratio"] * group["num_readings"]).sum() / total_readings
        statuses = set(group["status"])
        status = next(iter(statuses)) if len(statuses) == 1 else STATUS_MIXED
        return pd.Series(
            {"avg_ratio": weighted_avg, "status": status, "num_readings": total_readings}
        )

    grouped = working.groupby("h3_index", as_index=False).apply(_combine, include_groups=False)
    grouped["resolution"] = resolution
    return grouped[H3_CELL_COLUMNS].reset_index(drop=True)
