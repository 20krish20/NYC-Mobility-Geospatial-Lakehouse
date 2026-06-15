import pandas as pd
import pytest

from serving.api.h3_aggregation import aggregate_h3_cells
from streaming.h3_utils import H3_RESOLUTION

# Two resolution-9 cells that share the same resolution-7 parent
# ("872a100d2ffffff"), plus one cell from a different parent.
CHILD_A = "892a100d203ffff"
CHILD_B = "892a100d207ffff"
OTHER_CELL = "892a100da77ffff"

HOTSPOTS_DF = pd.DataFrame(
    [
        {
            "h3_index": CHILD_A,
            "borough": "Manhattan",
            "avg_ratio": 0.2,
            "status": "empty",
            "num_readings": 3,
        },
        {
            "h3_index": CHILD_B,
            "borough": "Manhattan",
            "avg_ratio": 0.4,
            "status": "normal",
            "num_readings": 1,
        },
        {
            "h3_index": OTHER_CELL,
            "borough": "Brooklyn",
            "avg_ratio": 0.9,
            "status": "full",
            "num_readings": 2,
        },
    ]
)


def test_resolution_equal_to_source_returns_rows_unchanged():
    result = aggregate_h3_cells(HOTSPOTS_DF, H3_RESOLUTION)

    assert set(result["h3_index"]) == {CHILD_A, CHILD_B, OTHER_CELL}
    assert (result["resolution"] == H3_RESOLUTION).all()


def test_coarser_resolution_aggregates_children_weighted_by_readings():
    result = aggregate_h3_cells(HOTSPOTS_DF, 7)

    by_cell = {row["h3_index"]: row for _, row in result.iterrows()}
    assert "872a100d2ffffff" in by_cell

    merged = by_cell["872a100d2ffffff"]
    assert merged["num_readings"] == 4
    # weighted avg: (0.2 * 3 + 0.4 * 1) / 4 = 0.25
    assert merged["avg_ratio"] == pytest.approx(0.25)
    # children disagree on status -> "mixed"
    assert merged["status"] == "mixed"
    assert merged["resolution"] == 7

    other = by_cell["872a100daffffff"]
    assert other["num_readings"] == 2
    assert other["avg_ratio"] == pytest.approx(0.9)
    assert other["status"] == "full"
    assert len(by_cell) == 2


def test_resolution_finer_than_source_raises():
    with pytest.raises(ValueError):
        aggregate_h3_cells(HOTSPOTS_DF, H3_RESOLUTION + 1)


def test_empty_dataframe_returns_empty_with_expected_columns():
    empty = HOTSPOTS_DF.iloc[0:0]

    result = aggregate_h3_cells(empty, 7)

    assert result.empty
    assert list(result.columns) == ["h3_index", "resolution", "avg_ratio", "status", "num_readings"]
