"""`fetch_jars` tests without network: matching the flattened name Ivy 2.5.2 generates.

The real risk is copying the wrong artifact — `commons-pool` 1.5.4 (which Spark already ships) in
place of `commons-pool2`, or another project's `kafka-clients`. `artifact_of` is what prevents that.
"""

from __future__ import annotations

from scripts.fetch_jars import artifact_of, version_key


def test_artifact_of_matches_the_flattened_ivy_names() -> None:
    names = {
        "org.apache.spark_spark-sql-kafka-0-10_2.13-4.3.0.jar": "spark-sql-kafka-0-10",
        "org.apache.spark_spark-token-provider-kafka-0-10_2.13-4.3.0.jar": (
            "spark-token-provider-kafka"
        ),
        "org.apache.kafka_kafka-clients-3.9.2.jar": "kafka-clients",
        "org.apache.commons_commons-pool2-2.13.1.jar": "commons-pool2",
    }
    for name, artifact in names.items():
        assert artifact_of(name) == artifact


def test_artifact_of_does_not_steal_the_spark_bundled_commons_pool() -> None:
    # Spark ships commons-pool 1.5.4; the connector requires pool2. Confusing the two breaks at
    # runtime (NoClassDefFoundError) after the job has already started.
    assert artifact_of("commons-pool-1.5.4.jar") is None
    assert artifact_of("hadoop-client-runtime-3.5.0.jar") is None


def test_version_key_orders_numerically() -> None:
    assert version_key("org.apache.kafka_kafka-clients-3.9.2.jar") > version_key(
        "org.apache.kafka_kafka-clients-3.8.10.jar"
    )
    assert version_key("no-version.jar") == (0,)
