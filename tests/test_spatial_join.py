from streaming.spatial_join import join_stations_to_boroughs, load_borough_polygons

# Well-known landmark points, each safely inside a single borough's
# (simplified) polygon, used to validate the point-in-polygon join.
LANDMARKS = [
    ("Empire State Building", 40.748817, -73.985428, "Manhattan"),
    ("Barclays Center", 40.682661, -73.975225, "Brooklyn"),
    ("Citi Field", 40.7571, -73.8458, "Queens"),
    ("Yankee Stadium", 40.8296, -73.9262, "Bronx"),
    ("Staten Island Ferry Terminal", 40.6437, -74.0734, "Staten Island"),
]


def test_load_borough_polygons_returns_all_five_boroughs(spark, boroughs_geojson_path):
    boroughs = load_borough_polygons(spark, boroughs_geojson_path)

    rows = boroughs.collect()
    assert len(rows) == 5
    names = {row["boroname"] for row in rows}
    assert names == {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"}


def test_join_stations_to_boroughs_assigns_known_landmarks(spark, boroughs_geojson_path):
    boroughs = load_borough_polygons(spark, boroughs_geojson_path)
    points = spark.createDataFrame(
        [(name, lat, lon) for name, lat, lon, _ in LANDMARKS],
        ["name", "lat", "lon"],
    )

    result = join_stations_to_boroughs(points, boroughs).collect()

    by_name = {row["name"]: row["borough"] for row in result}
    for name, _lat, _lon, expected_borough in LANDMARKS:
        assert by_name[name] == expected_borough


def test_join_stations_to_boroughs_null_for_point_outside_nyc(spark, boroughs_geojson_path):
    boroughs = load_borough_polygons(spark, boroughs_geojson_path)
    # Somewhere in the Atlantic Ocean, far from any borough polygon.
    points = spark.createDataFrame([("Ocean", 40.0, -70.0)], ["name", "lat", "lon"])

    result = join_stations_to_boroughs(points, boroughs).collect()

    assert result[0]["borough"] is None
