import h3
import pandas as pd

from serving.dashboard.app import STATUS_COLORS, h3_cell_polygon, imbalance_color


def test_imbalance_color_empty_is_red():
    assert imbalance_color(0.0) == STATUS_COLORS["empty"]


def test_imbalance_color_full_is_blue():
    assert imbalance_color(1.0) == STATUS_COLORS["full"]


def test_imbalance_color_balanced_is_normal():
    assert imbalance_color(0.5) == STATUS_COLORS["normal"]


def test_imbalance_color_clamps_out_of_range_values():
    assert imbalance_color(-1.0) == imbalance_color(0.0)
    assert imbalance_color(2.0) == imbalance_color(1.0)


def test_imbalance_color_nan_is_mixed_gray():
    assert imbalance_color(float("nan")) == STATUS_COLORS["mixed"]
    assert imbalance_color(pd.NA) == STATUS_COLORS["mixed"]


def test_h3_cell_polygon_returns_lon_lat_ring_matching_boundary():
    cell = "892a100d2d7ffff"
    polygon = h3_cell_polygon(cell)
    boundary = h3.cell_to_boundary(cell)

    assert len(polygon) == len(boundary)
    for (lon, lat), (boundary_lat, boundary_lon) in zip(polygon, boundary, strict=True):
        assert lon == boundary_lon
        assert lat == boundary_lat
