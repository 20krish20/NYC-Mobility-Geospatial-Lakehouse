from streaming.h3_utils import H3_RESOLUTION, lat_lon_to_h3, with_h3_index

# Fixed Citi Bike station coordinates (data/sample/station_information_sample.json)
# with their resolution-9 H3 cells computed once via h3.latlng_to_cell.
W21_ST_6_AVE = (40.74173, -73.99416, "892a100d247ffff")
WYTHE_AVE_METROPOLITAN_AVE = (40.71594678, -73.96334072, "892a100d337ffff")


def test_lat_lon_to_h3_known_points():
    lat, lon, expected_cell = W21_ST_6_AVE
    assert lat_lon_to_h3(lat, lon, H3_RESOLUTION) == expected_cell

    lat, lon, expected_cell = WYTHE_AVE_METROPOLITAN_AVE
    assert lat_lon_to_h3(lat, lon, H3_RESOLUTION) == expected_cell


def test_lat_lon_to_h3_is_deterministic():
    lat, lon, _ = W21_ST_6_AVE
    assert lat_lon_to_h3(lat, lon) == lat_lon_to_h3(lat, lon)


def test_lat_lon_to_h3_different_points_different_cells():
    lat1, lon1, _ = W21_ST_6_AVE
    lat2, lon2, _ = WYTHE_AVE_METROPOLITAN_AVE
    assert lat_lon_to_h3(lat1, lon1) != lat_lon_to_h3(lat2, lon2)


def test_lat_lon_to_h3_handles_missing_coordinates():
    assert lat_lon_to_h3(None, -73.99416) is None
    assert lat_lon_to_h3(40.74173, None) is None
    assert lat_lon_to_h3(None, None) is None


def test_with_h3_index_adds_expected_column(spark):
    df = spark.createDataFrame(
        [
            ("519", *W21_ST_6_AVE[:2]),
            ("382", *WYTHE_AVE_METROPOLITAN_AVE[:2]),
        ],
        ["station_id", "lat", "lon"],
    )

    result = with_h3_index(df).collect()

    by_id = {row["station_id"]: row["h3_index"] for row in result}
    assert by_id["519"] == W21_ST_6_AVE[2]
    assert by_id["382"] == WYTHE_AVE_METROPOLITAN_AVE[2]
