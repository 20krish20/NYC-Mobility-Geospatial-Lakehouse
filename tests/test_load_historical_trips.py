from batch.load_historical_trips import enrich_trips, read_trips, with_trip_duration
from streaming.spatial_join import load_borough_polygons

TRIPS_CSV = "data/sample/citibike_trips_sample.csv"

# Expected H3 res-9 cells for the landmark coordinates used in the sample CSV.
EMPIRE_STATE_H3 = "892a100d2d7ffff"
BARCLAYS_H3 = "892a100da77ffff"
CITI_FIELD_H3 = "892a100e237ffff"
YANKEE_STADIUM_H3 = "892a100a86fffff"
STATEN_ISLAND_FERRY_H3 = "892a1070bb7ffff"


def test_with_trip_duration_computes_seconds(spark):
    trips = read_trips(spark, TRIPS_CSV)

    result = with_trip_duration(trips).collect()

    by_id = {row["ride_id"]: row["trip_duration_seconds"] for row in result}
    assert by_id["T1"] == 15 * 60
    assert by_id["T6"] == 5 * 60


def test_enrich_trips_assigns_h3_cells_and_boroughs(spark, boroughs_geojson_path):
    trips = read_trips(spark, TRIPS_CSV)
    boroughs = load_borough_polygons(spark, boroughs_geojson_path)

    result = enrich_trips(trips, boroughs).collect()
    by_id = {row["ride_id"]: row for row in result}

    # T1: Empire State Building (Manhattan) -> Barclays Center (Brooklyn)
    assert by_id["T1"]["origin_h3"] == EMPIRE_STATE_H3
    assert by_id["T1"]["destination_h3"] == BARCLAYS_H3
    assert by_id["T1"]["origin_borough"] == "Manhattan"
    assert by_id["T1"]["destination_borough"] == "Brooklyn"

    # T3: Citi Field (Queens) -> Yankee Stadium (Bronx)
    assert by_id["T3"]["origin_h3"] == CITI_FIELD_H3
    assert by_id["T3"]["destination_h3"] == YANKEE_STADIUM_H3
    assert by_id["T3"]["origin_borough"] == "Queens"
    assert by_id["T3"]["destination_borough"] == "Bronx"

    # T5: Staten Island Ferry Terminal -> Empire State Building (Manhattan)
    assert by_id["T5"]["origin_h3"] == STATEN_ISLAND_FERRY_H3
    assert by_id["T5"]["destination_h3"] == EMPIRE_STATE_H3
    assert by_id["T5"]["origin_borough"] == "Staten Island"
    assert by_id["T5"]["destination_borough"] == "Manhattan"

    # T6: same start/end point -> same H3 cell and borough on both sides.
    assert by_id["T6"]["origin_h3"] == by_id["T6"]["destination_h3"] == EMPIRE_STATE_H3
    assert by_id["T6"]["origin_borough"] == by_id["T6"]["destination_borough"] == "Manhattan"
