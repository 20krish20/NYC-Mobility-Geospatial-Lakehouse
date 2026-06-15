from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from data_quality.expectations import apply_expectations

SCHEMA = StructType(
    [
        StructField("station_id", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("capacity", IntegerType()),
        StructField("h3_index", StringType()),
        StructField("borough", StringType()),
    ]
)

VALID_ROW = ("519", 40.74173, -73.99416, 39, "892a100d247ffff", "Manhattan")


def test_valid_row_passes_all_checks(spark):
    df = spark.createDataFrame([VALID_ROW], SCHEMA)

    valid, quarantined = apply_expectations(df)

    assert valid.count() == 1
    assert quarantined.count() == 0
    assert "_dq_failures" not in valid.columns


def test_null_required_field_is_quarantined(spark):
    row = ("519", 40.74173, -73.99416, 39, "892a100d247ffff", None)
    df = spark.createDataFrame([row], SCHEMA)

    valid, quarantined = apply_expectations(df)

    assert valid.count() == 0
    failures = quarantined.collect()[0]["_dq_failures"]
    assert "borough_not_null" in failures


def test_lat_lon_outside_nyc_bbox_is_quarantined(spark):
    row = ("999", 51.5074, -0.1278, 20, "892a100d247ffff", "Manhattan")  # London
    df = spark.createDataFrame([row], SCHEMA)

    valid, quarantined = apply_expectations(df)

    assert valid.count() == 0
    failures = quarantined.collect()[0]["_dq_failures"]
    assert "lat_within_nyc_bbox" in failures
    assert "lon_within_nyc_bbox" in failures


def test_non_positive_capacity_is_quarantined(spark):
    row = ("519", 40.74173, -73.99416, 0, "892a100d247ffff", "Manhattan")
    df = spark.createDataFrame([row], SCHEMA)

    valid, quarantined = apply_expectations(df)

    assert valid.count() == 0
    failures = quarantined.collect()[0]["_dq_failures"]
    assert failures == ["capacity_positive"]


def test_multiple_failures_are_all_reported(spark):
    row = (None, 51.5074, -0.1278, 0, None, None)
    df = spark.createDataFrame([row], SCHEMA)

    valid, quarantined = apply_expectations(df)

    failures = set(quarantined.collect()[0]["_dq_failures"])
    assert failures == {
        "station_id_not_null",
        "h3_index_not_null",
        "borough_not_null",
        "lat_within_nyc_bbox",
        "lon_within_nyc_bbox",
        "capacity_positive",
    }
