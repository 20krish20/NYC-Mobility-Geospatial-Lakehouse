import json

from ingestion.gbfs_poller import parse_station_information, write_station_information
from streaming.spatial_join import load_borough_polygons
from streaming.station_status_stream import (
    STATION_INFO_SCHEMA,
    STATION_STATUS_SCHEMA,
    compute_imbalance_ratio,
    enrich_station_status,
    load_station_information,
)


def test_compute_imbalance_ratio_basic(spark):
    df = spark.createDataFrame(
        [(8, 20), (0, 0), (5, None)],
        ["num_bikes_available", "capacity"],
    )

    result = compute_imbalance_ratio(df).collect()

    by_capacity = {row["capacity"]: row["imbalance_ratio"] for row in result}
    assert by_capacity[20] == 0.4
    assert by_capacity[0] is None
    assert by_capacity[None] is None


def test_load_station_information_from_poller_reference_file(spark, sample_data_dir, tmp_path):
    sample = json.loads((sample_data_dir / "station_information_sample.json").read_text())
    stations = parse_station_information(sample)
    out_path = write_station_information(stations, str(tmp_path / "ref"))

    df = load_station_information(spark, str(out_path))

    rows = {row["station_id"]: row for row in df.collect()}
    assert rows["519"]["name"] == "W 21 St & 6 Ave"
    assert rows["519"]["capacity"] == 39
    assert rows["382"]["lat"] == 40.71594678


def test_enrich_station_status(spark, boroughs_geojson_path):
    station_info_df = spark.createDataFrame(
        [
            ("519", "W 21 St & 6 Ave", 40.74173, -73.99416, 39),
            ("382", "Wythe Ave & Metropolitan Ave", 40.71594678, -73.96334072, 31),
        ],
        schema=STATION_INFO_SCHEMA,
    )
    status_df = spark.createDataFrame(
        [
            ("519", 12, 27, 1, 1, 1700000050),
            ("382", 0, 31, 1, 1, 1700000040),
        ],
        schema=STATION_STATUS_SCHEMA,
    )
    boroughs_df = load_borough_polygons(spark, boroughs_geojson_path)

    enriched = enrich_station_status(status_df, station_info_df, boroughs_df).collect()
    by_id = {row["station_id"]: row for row in enriched}

    assert by_id["519"]["h3_index"] == "892a100d247ffff"
    assert by_id["519"]["borough"] == "Manhattan"
    assert by_id["519"]["imbalance_ratio"] == 12 / 39

    assert by_id["382"]["h3_index"] == "892a100d337ffff"
    assert by_id["382"]["borough"] == "Brooklyn"
    assert by_id["382"]["imbalance_ratio"] == 0.0
