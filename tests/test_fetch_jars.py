"""Testes do `fetch_jars` sem rede: o casamento do nome achatado que o Ivy 2.5.2 gera.

O risco real é copiar o artefato errado — `commons-pool` 1.5.4 (que o Spark já traz) no lugar de
`commons-pool2`, ou a `kafka-clients` de outro projeto. `artifact_of` é o que impede isso.
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
    # O Spark distribui commons-pool 1.5.4; o conector exige o pool2. Confundir os dois quebra em
    # runtime (NoClassDefFoundError) já depois do job começar.
    assert artifact_of("commons-pool-1.5.4.jar") is None
    assert artifact_of("hadoop-client-runtime-3.5.0.jar") is None


def test_version_key_orders_numerically() -> None:
    assert version_key("org.apache.kafka_kafka-clients-3.9.2.jar") > version_key(
        "org.apache.kafka_kafka-clients-3.8.10.jar"
    )
    assert version_key("sem-versao.jar") == (0,)
