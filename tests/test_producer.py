"""Testes do replay: ordenação, ritmo (`pace`), flags de injeção e contabilidade do run."""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import producer.producer as replay
from producer.schema import EVENT_FIELDS


def write_parquet(path: Path, pickups: list[datetime]) -> Path:
    """Parquet mínimo (colunas do TLC) com uma linha por pickup, na ordem dada."""
    rows = [
        {
            "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": pickup.replace(minute=pickup.minute + 10),
            "PULocationID": 114,
            "DOLocationID": 48,
            "fare_amount": 10.0,
            "tip_amount": 1.0,
            "total_amount": 12.0,
            "payment_type": 1,
            "trip_distance": 2.0,
        }
        for pickup in pickups
    ]
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_iter_rows_orders_by_pickup(tmp_path: Path) -> None:
    path = write_parquet(
        tmp_path / "mes.parquet",
        [
            datetime(2024, 1, 1, 0, 3),
            datetime(2024, 1, 1, 0, 1),
            datetime(2024, 1, 1, 0, 5),
            datetime(2024, 1, 1, 0, 2),
        ],
    )

    pickups = [row["tpep_pickup_datetime"] for row in replay.iter_rows(path)]

    assert pickups == sorted(pickups)
    assert len(pickups) == 4


def test_pace_sleeps_only_the_remaining_time(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr(time, "monotonic", lambda: 100.5)
    monkeypatch.setattr(time, "sleep", slept.append)

    replay.pace(started=100.0, sent=10, rate=10.0)  # deadline 101.0
    assert slept == [pytest.approx(0.5)]

    slept.clear()
    replay.pace(started=100.0, sent=3, rate=10.0)  # deadline 100.3: já passou, não dorme
    replay.pace(started=100.0, sent=10_000, rate=0.0)  # sem freio
    replay.pace(started=100.0, sent=504, rate=1_000.0)  # falta 4 ms: não dorme
    assert slept == []


@pytest.mark.parametrize("flag", ["--rate", "--limit", "--dupes", "--dirty"])
def test_parse_args_rejects_negative(flag: str) -> None:
    with pytest.raises(SystemExit):
        replay.parse_args([flag, "-1"])


def test_parse_args_caps_dirty_ahead_of_the_late_window() -> None:
    with pytest.raises(SystemExit):
        replay.parse_args(["--dirty", str(replay.MAX_DIRTY_AHEAD_S + 1)])


class FakeProducer:
    """Substitui o confluent-kafka: registra as mensagens e confirma tudo na hora."""

    instances: ClassVar[list[FakeProducer]] = []

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.messages: list[tuple[str, bytes, bytes]] = []
        FakeProducer.instances.append(self)

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.messages.append((topic, key, value))
        on_delivery(None, None)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


class FakeMsg:
    """O callback de erro só usa topic/partition/offset para a amostra."""

    def __init__(self, topic: str) -> None:
        self._topic = topic

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0


class FailingProducer(FakeProducer):
    """Broker que recusa tudo: exercita o contador de falhas e o exit 1."""

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        self.messages.append((topic, key, value))
        on_delivery(RuntimeError("broker fora do ar"), FakeMsg(topic))


def run_replay(monkeypatch: pytest.MonkeyPatch, path: Path, *extra: str) -> int:
    FakeProducer.instances.clear()
    monkeypatch.setattr(replay, "Producer", FakeProducer)
    args = replay.parse_args(
        [
            "--files",
            str(path),
            "--rate",
            "0",
            "--limit",
            "3",
            "--log-every",
            "600",
            *extra,
        ]
    )
    return replay.run(args)


def test_run_injects_dupes_and_dirty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = write_parquet(
        tmp_path / "mes.parquet",
        [
            datetime(2024, 1, 1, 0, 3),
            datetime(2024, 1, 1, 0, 1),
            datetime(2024, 1, 1, 0, 2),
        ],
    )

    assert run_replay(monkeypatch, path, "--dupes", "2", "--dirty", "4") == 0

    messages = FakeProducer.instances[-1].messages
    events = [json.loads(value) for _, _, value in messages]
    # ordem de envio: real, duplicata, sujo(0), real, duplicata, sujo(1), real, sujo(2), sujo(3)
    assert len(events) == 3 + 2 + 4
    assert events[1] == events[0]
    assert events[4] == events[3]
    assert [event["event_time"] for event in events[::3]] == [
        "2024-01-01T00:01:00",
        "2024-01-01T00:02:00",
        "2024-01-01T00:03:00",
    ]
    for event in events:
        assert tuple(event) == EVENT_FIELDS

    dirty = [events[2], events[5], events[7], events[8]]
    assert len({event["trip_id"] for event in dirty}) == 4
    assert dirty[0]["pickup_zone_id"] is None
    assert dirty[1]["dropoff_at"] == dirty[1]["event_time"]
    assert dirty[2]["fare_amount"] < 0
    assert dirty[3]["total_amount"] > 10_000


def test_run_fails_when_limit_is_not_reached(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = write_parquet(tmp_path / "mes.parquet", [datetime(2024, 1, 1, 0, 1)])

    FakeProducer.instances.clear()
    monkeypatch.setattr(replay, "Producer", FakeProducer)
    args = replay.parse_args(["--files", str(path), "--rate", "0", "--limit", "10"])
    assert replay.run(args) == 1


def test_run_fails_when_delivery_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = write_parquet(
        tmp_path / "mes.parquet",
        [datetime(2024, 1, 1, 0, 1), datetime(2024, 1, 1, 0, 2)],
    )

    FakeProducer.instances.clear()
    monkeypatch.setattr(replay, "Producer", FailingProducer)
    args = replay.parse_args(["--files", str(path), "--rate", "0", "--limit", "2"])

    assert replay.run(args) == 1
    assert len(FakeProducer.instances[-1].messages) == 2
