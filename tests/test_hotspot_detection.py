from streaming.hotspot_detection import (
    STATUS_EMPTY,
    STATUS_FULL,
    STATUS_NORMAL,
    aggregate_hotspot_windows,
    compute_gold_hotspots,
    with_event_time,
)

# 2024-01-01T00:00:00Z, aligned to a 15-minute tumbling-window boundary.
WINDOW_START = 1704067200
WINDOW_SIZE_SECONDS = 15 * 60

COLUMNS = ["h3_index", "borough", "imbalance_ratio", "last_reported"]


def _ts(offset_seconds: int) -> int:
    return WINDOW_START + offset_seconds


def test_with_event_time_converts_epoch_seconds_to_timestamp(spark):
    df = spark.createDataFrame([("cell", "Manhattan", 0.5, _ts(0))], COLUMNS)

    result = with_event_time(df).collect()[0]

    assert result["event_time"].timestamp() == _ts(0)


def test_compute_gold_hotspots_flags_sustained_empty_and_full_cells(spark):
    rows = [
        # Persistently near-empty for the whole window -> "empty" hotspot.
        ("empty_cell", "Manhattan", 0.05, _ts(0)),
        ("empty_cell", "Manhattan", 0.02, _ts(300)),
        ("empty_cell", "Manhattan", 0.08, _ts(600)),
        # Persistently near-full for the whole window -> "full" hotspot.
        ("full_cell", "Brooklyn", 0.95, _ts(120)),
        ("full_cell", "Brooklyn", 0.92, _ts(420)),
        ("full_cell", "Brooklyn", 0.99, _ts(720)),
        # Swings from empty to full within the window -> not sustained.
        ("mixed_cell", "Queens", 0.05, _ts(0)),
        ("mixed_cell", "Queens", 0.95, _ts(600)),
        # Stays mid-range -> normal.
        ("normal_cell", "Bronx", 0.5, _ts(0)),
        ("normal_cell", "Bronx", 0.6, _ts(600)),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = {row["h3_index"]: row for row in compute_gold_hotspots(df).collect()}

    assert result["empty_cell"]["status"] == STATUS_EMPTY
    assert result["full_cell"]["status"] == STATUS_FULL
    assert result["mixed_cell"]["status"] == STATUS_NORMAL
    assert result["normal_cell"]["status"] == STATUS_NORMAL


def test_aggregate_hotspot_windows_computes_avg_min_max_count(spark):
    rows = [
        ("cell_a", "Manhattan", 0.2, _ts(0)),
        ("cell_a", "Manhattan", 0.4, _ts(300)),
        ("cell_a", "Manhattan", 0.6, _ts(600)),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = aggregate_hotspot_windows(with_event_time(df)).collect()[0]

    assert result["num_readings"] == 3
    assert result["min_ratio"] == 0.2
    assert result["max_ratio"] == 0.6
    assert abs(result["avg_ratio"] - 0.4) < 1e-9


def test_compute_gold_hotspots_separates_consecutive_windows(spark):
    rows = [
        ("cell_x", "Manhattan", 0.05, _ts(0)),
        ("cell_x", "Manhattan", 0.05, _ts(WINDOW_SIZE_SECONDS + 60)),
    ]
    df = spark.createDataFrame(rows, COLUMNS)

    result = compute_gold_hotspots(df).collect()

    assert len(result) == 2
    starts = sorted(row["window_start"] for row in result)
    assert starts[1] > starts[0]
