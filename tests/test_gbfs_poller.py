import json
from pathlib import Path

import pytest
import requests
import responses

from ingestion import gbfs_poller

SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample"


def _load_sample(name: str) -> dict:
    return json.loads((SAMPLE_DIR / name).read_text())


class FakeProducer:
    """In-memory stand-in for confluent_kafka.Producer used in tests."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.flushed = False

    def produce(self, topic, key=None, value=None, callback=None) -> None:
        self.messages.append((topic, key, value))
        if callback is not None:
            callback(None, _FakeMessage(key))

    def flush(self) -> None:
        self.flushed = True


class _FakeMessage:
    def __init__(self, key: bytes) -> None:
        self._key = key

    def key(self) -> bytes:
        return self._key


def test_parse_station_status_extracts_records():
    payload = _load_sample("station_status_sample.json")

    stations = gbfs_poller.parse_station_status(payload)

    assert len(stations) == 3
    by_id = {s["station_id"]: s for s in stations}
    assert by_id["519"]["num_bikes_available"] == 12
    assert by_id["382"]["num_docks_available"] == 31


def test_parse_station_information_extracts_records():
    payload = _load_sample("station_information_sample.json")

    stations = gbfs_poller.parse_station_information(payload)

    assert len(stations) == 3
    by_id = {s["station_id"]: s for s in stations}
    assert by_id["519"]["name"] == "W 21 St & 6 Ave"
    assert by_id["470"]["capacity"] == 24


def test_parse_handles_missing_data_key():
    assert gbfs_poller.parse_station_status({}) == []
    assert gbfs_poller.parse_station_information({}) == []


@responses.activate
def test_fetch_json_success():
    url = "https://example.com/station_status.json"
    payload = _load_sample("station_status_sample.json")
    responses.add(responses.GET, url, json=payload, status=200)

    with requests.Session() as session:
        result = gbfs_poller.fetch_json(url, session=session, max_retries=3, backoff_seconds=0)

    assert result == payload


@responses.activate
def test_fetch_json_retries_then_succeeds(monkeypatch):
    url = "https://example.com/station_status.json"
    payload = _load_sample("station_status_sample.json")
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, json=payload, status=200)

    sleeps: list[float] = []
    monkeypatch.setattr(gbfs_poller.time, "sleep", lambda s: sleeps.append(s))

    with requests.Session() as session:
        result = gbfs_poller.fetch_json(url, session=session, max_retries=5, backoff_seconds=0.01)

    assert result == payload
    # Two retries before success -> two backoff sleeps
    assert len(sleeps) == 2


@responses.activate
def test_fetch_json_raises_after_max_retries(monkeypatch):
    url = "https://example.com/station_status.json"
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, url, status=500)

    monkeypatch.setattr(gbfs_poller.time, "sleep", lambda s: None)

    with requests.Session() as session:
        with pytest.raises(requests.RequestException):
            gbfs_poller.fetch_json(url, session=session, max_retries=3, backoff_seconds=0.01)


def test_publish_station_status_publishes_each_record_keyed_by_station_id():
    payload = _load_sample("station_status_sample.json")
    stations = gbfs_poller.parse_station_status(payload)
    producer = FakeProducer()

    count = gbfs_poller.publish_station_status(producer, "station-status", stations)

    assert count == 3
    assert producer.flushed is True
    topics = {msg[0] for msg in producer.messages}
    assert topics == {"station-status"}
    keys = {msg[1] for msg in producer.messages}
    assert keys == {b"519", b"382", b"470"}

    by_key = {msg[1]: json.loads(msg[2]) for msg in producer.messages}
    assert by_key[b"519"]["num_bikes_available"] == 12


def test_write_station_information_writes_json_file(tmp_path):
    payload = _load_sample("station_information_sample.json")
    stations = gbfs_poller.parse_station_information(payload)

    out_path = gbfs_poller.write_station_information(stations, str(tmp_path / "ref"))

    assert out_path.exists()
    written = json.loads(out_path.read_text())
    assert len(written) == 3
    assert {s["station_id"] for s in written} == {"519", "382", "470"}


@responses.activate
def test_poll_status_once_publishes_all_stations():
    config = gbfs_poller.PollerConfig(gbfs_base_url="https://example.com")
    payload = _load_sample("station_status_sample.json")
    responses.add(responses.GET, config.station_status_url, json=payload, status=200)

    producer = FakeProducer()
    with requests.Session() as session:
        published = gbfs_poller.poll_status_once(config, session, producer)

    assert published == 3


def test_poller_config_from_env_uses_defaults_and_overrides():
    default_config = gbfs_poller.PollerConfig.from_env(env={})
    assert default_config.gbfs_base_url == gbfs_poller.DEFAULT_GBFS_BASE_URL
    assert default_config.poll_interval_seconds == gbfs_poller.DEFAULT_POLL_INTERVAL_SECONDS

    override_config = gbfs_poller.PollerConfig.from_env(
        env={
            "GBFS_BASE_URL": "https://example.com/gbfs",
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_TOPIC": "custom-topic",
            "POLL_INTERVAL_SECONDS": "30",
        }
    )
    assert override_config.gbfs_base_url == "https://example.com/gbfs"
    assert override_config.kafka_bootstrap_servers == "broker:9092"
    assert override_config.kafka_topic == "custom-topic"
    assert override_config.poll_interval_seconds == 30.0
    assert override_config.station_status_url == "https://example.com/gbfs/station_status.json"
