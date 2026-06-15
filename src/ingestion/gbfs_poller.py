"""Poll the Citi Bike GBFS feed and publish station status to Kafka.

Two GBFS endpoints are involved:

- ``station_information.json`` is effectively static (station id, name,
  lat/lon, capacity). It is fetched periodically and written to a local
  reference file so the downstream Spark streaming job can broadcast-join
  against it without hitting the network itself.
- ``station_status.json`` changes constantly (bikes/docks available) and is
  polled every ``poll_interval_seconds`` and published to the Kafka topic
  ``station-status``, one message per station, keyed by ``station_id``.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import requests
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

DEFAULT_GBFS_BASE_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/en"
DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_KAFKA_TOPIC = "station-status"
DEFAULT_POLL_INTERVAL_SECONDS = 60.0
DEFAULT_STATION_INFO_REFRESH_SECONDS = 3600.0
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_SECONDS = 1.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
DEFAULT_REFERENCE_DIR = "data/reference"


@dataclass(frozen=True)
class PollerConfig:
    gbfs_base_url: str = DEFAULT_GBFS_BASE_URL
    kafka_bootstrap_servers: str = DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    kafka_topic: str = DEFAULT_KAFKA_TOPIC
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    station_info_refresh_seconds: float = DEFAULT_STATION_INFO_REFRESH_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS
    request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    reference_dir: str = DEFAULT_REFERENCE_DIR

    @classmethod
    def from_env(cls, env: dict | None = None) -> PollerConfig:
        import os

        env = env if env is not None else os.environ
        return cls(
            gbfs_base_url=env.get("GBFS_BASE_URL", DEFAULT_GBFS_BASE_URL),
            kafka_bootstrap_servers=env.get(
                "KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS
            ),
            kafka_topic=env.get("KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC),
            poll_interval_seconds=float(
                env.get("POLL_INTERVAL_SECONDS", DEFAULT_POLL_INTERVAL_SECONDS)
            ),
            station_info_refresh_seconds=float(
                env.get("STATION_INFO_REFRESH_SECONDS", DEFAULT_STATION_INFO_REFRESH_SECONDS)
            ),
            max_retries=int(env.get("GBFS_MAX_RETRIES", DEFAULT_MAX_RETRIES)),
            backoff_seconds=float(env.get("GBFS_BACKOFF_SECONDS", DEFAULT_BACKOFF_SECONDS)),
            request_timeout_seconds=float(
                env.get("GBFS_REQUEST_TIMEOUT_SECONDS", DEFAULT_REQUEST_TIMEOUT_SECONDS)
            ),
            reference_dir=env.get("REFERENCE_DIR", DEFAULT_REFERENCE_DIR),
        )

    @property
    def station_information_url(self) -> str:
        return f"{self.gbfs_base_url}/station_information.json"

    @property
    def station_status_url(self) -> str:
        return f"{self.gbfs_base_url}/station_status.json"


def fetch_json(
    url: str,
    *,
    session: requests.Session,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Fetch a JSON document, retrying with exponential backoff on failure.

    Raises the last encountered exception if all attempts fail.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                sleep_for = backoff_seconds * (2**attempt)
                logger.warning(
                    "Fetch failed for %s (attempt %d/%d): %s. Retrying in %.1fs",
                    url,
                    attempt + 1,
                    max_retries,
                    exc,
                    sleep_for,
                )
                time.sleep(sleep_for)
    assert last_exc is not None
    raise last_exc


def parse_station_status(payload: dict) -> list[dict]:
    """Extract the list of station status records from a station_status.json payload."""
    return payload.get("data", {}).get("stations", [])


def parse_station_information(payload: dict) -> list[dict]:
    """Extract the list of station info records from a station_information.json payload."""
    return payload.get("data", {}).get("stations", [])


def write_station_information(stations: list[dict], reference_dir: str) -> Path:
    """Write station_information records to a local JSON reference file.

    Returns the path written to.
    """
    out_dir = Path(reference_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "station_information.json"
    out_path.write_text(json.dumps(stations, indent=2))
    return out_path


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers})


def _delivery_callback(err, msg) -> None:
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)


def publish_station_status(producer: Producer, topic: str, stations: Iterable[dict]) -> int:
    """Publish each station status record to Kafka, keyed by station_id.

    Returns the number of records published.
    """
    count = 0
    for station in stations:
        key = station.get("station_id", "")
        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(station).encode("utf-8"),
            callback=_delivery_callback,
        )
        count += 1
    producer.flush()
    return count


def refresh_station_information(config: PollerConfig, session: requests.Session) -> int:
    """Fetch station_information.json and write it to the reference directory.

    Returns the number of stations written.
    """
    payload = fetch_json(
        config.station_information_url,
        session=session,
        max_retries=config.max_retries,
        backoff_seconds=config.backoff_seconds,
        timeout=config.request_timeout_seconds,
    )
    stations = parse_station_information(payload)
    write_station_information(stations, config.reference_dir)
    logger.info("Refreshed station_information.json (%d stations)", len(stations))
    return len(stations)


def poll_status_once(config: PollerConfig, session: requests.Session, producer: Producer) -> int:
    """Fetch station_status.json once and publish the records to Kafka.

    Returns the number of records published.
    """
    payload = fetch_json(
        config.station_status_url,
        session=session,
        max_retries=config.max_retries,
        backoff_seconds=config.backoff_seconds,
        timeout=config.request_timeout_seconds,
    )
    stations = parse_station_status(payload)
    published = publish_station_status(producer, config.kafka_topic, stations)
    logger.info("Published %d station status records to %s", published, config.kafka_topic)
    return published


def run(config: PollerConfig | None = None) -> None:
    """Run the poll loop forever: refresh station info periodically, poll status every interval."""
    config = config or PollerConfig.from_env()
    session = requests.Session()
    producer = build_producer(config.kafka_bootstrap_servers)

    logger.info(
        "Starting GBFS poller: %s -> kafka topic '%s' (bootstrap=%s) every %.0fs",
        config.gbfs_base_url,
        config.kafka_topic,
        config.kafka_bootstrap_servers,
        config.poll_interval_seconds,
    )

    last_info_refresh = 0.0
    while True:
        now = time.monotonic()
        if now - last_info_refresh >= config.station_info_refresh_seconds:
            try:
                refresh_station_information(config, session)
                last_info_refresh = now
            except Exception:
                logger.exception("Failed to refresh station_information.json")

        try:
            poll_status_once(config, session, producer)
        except Exception:
            logger.exception("Poll cycle failed; will retry next interval")

        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    run()
