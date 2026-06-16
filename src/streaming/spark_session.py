"""Shared SparkSession factory configured for Sedona (spatial SQL) + Delta Lake.

Versions must stay in sync with the ``pyspark`` / ``delta-spark`` /
``apache-sedona`` pins in ``pyproject.toml``:

- pyspark==3.5.1
- delta-spark==3.2.0
- apache-sedona==1.6.1 (Scala 2.12 build)
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

SEDONA_PACKAGE = "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.6.1"
GEOTOOLS_PACKAGE = "org.datasyslab:geotools-wrapper:1.6.1-28.2"
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

MAVEN_PACKAGES = ",".join([SEDONA_PACKAGE, GEOTOOLS_PACKAGE, DELTA_PACKAGE, KAFKA_PACKAGE])

SQL_EXTENSIONS = ",".join(
    [
        "org.apache.sedona.sql.SedonaSqlExtensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    ]
)


def get_spark_session(
    app_name: str = "nyc-mobility-streaming", master: str = "local[*]"
) -> SparkSession:
    """Create (or reuse) a SparkSession with Sedona spatial SQL and Delta Lake enabled."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", MAVEN_PACKAGES)
        .config("spark.sql.extensions", SQL_EXTENSIONS)
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Avoid the AdminClient "describeTopics" call that can't reach the broker's
        # internal advertised listener (kafka:9092) from outside Docker.
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "true")
    )
    return SedonaContext.create(builder.getOrCreate())
