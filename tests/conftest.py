from pathlib import Path

import pytest

from streaming.spark_session import get_spark_session

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session(app_name="nyc-mobility-tests", master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def boroughs_geojson_path() -> str:
    return str(DATA_DIR / "geo" / "nyc_boroughs.geojson")


@pytest.fixture(scope="session")
def sample_data_dir() -> Path:
    return DATA_DIR / "sample"
