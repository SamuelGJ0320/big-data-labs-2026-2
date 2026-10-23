"""Autoevaluación de producer.py (no necesita Kafka ni AWS). No modifiques este archivo."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))

import producer  # noqa: E402


def event(customer="C000001", txn="TXN1", event_type="payment"):
    return {"event_id": f"E-{txn}-{event_type}", "event_type": event_type, "transaction_id": txn,
            "customer_id": customer, "amount_cop": 50_000.0}


# ── TODO 1: partition key ────────────────────────────────────────────────────

def test_partition_key_mismo_cliente_misma_particion():
    assert producer.partition_key(event(txn="T1")) == producer.partition_key(event(txn="T2")), \
        "los pagos de un mismo cliente (por ejemplo, una ráfaga) deben ir a la misma partición"


def test_partition_key_contracargo_con_su_pago():
    assert producer.partition_key(event(txn="T1")) == producer.partition_key(event(txn="T1", event_type="chargeback")), \
        "el contracargo debe llegar después de su pago, en la misma partición"


def test_partition_key_reparte_la_carga():
    keys = {producer.partition_key(event(customer=f"C{i:06d}", txn=f"T{i}")) for i in range(100)}
    assert len(keys) > 50, "la llave debe repartir la carga entre particiones"
    assert all(isinstance(k, str) for k in keys)


# ── TODO 2: configuración de Kafka ───────────────────────────────────────────

def test_kafka_config():
    config = producer.kafka_config("localhost:9092")
    assert config["bootstrap.servers"] == "localhost:9092"
    assert str(config.get("acks")) in {"all", "-1"}, "acks: el líder y las réplicas deben confirmar el evento"
    assert config.get("enable.idempotence") in {True, "true"}, "los reintentos no deben duplicar eventos"


# ── TODO 3: reintentos en Kinesis ────────────────────────────────────────────

class FakeKinesis:
    """Rechaza los registros indicados en sus primeros `fail_times` intentos."""

    def __init__(self, fail_keys, fail_times):
        self.fail_keys, self.fail_times, self.calls = set(fail_keys), fail_times, []

    def put_records(self, StreamName, Records):
        self.calls.append([r["PartitionKey"] for r in Records])
        failing = len(self.calls) <= self.fail_times
        results = [
            {"ErrorCode": "ProvisionedThroughputExceededException"}
            if failing and r["PartitionKey"] in self.fail_keys else {"SequenceNumber": "1", "ShardId": "s"}
            for r in Records
        ]
        return {"FailedRecordCount": sum("ErrorCode" in x for x in results), "Records": results}


def records(*keys):
    return [{"Data": b"{}", "PartitionKey": k} for k in keys]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(producer.time, "sleep", lambda s: None)


def test_reintenta_solo_los_fallidos():
    k = FakeKinesis(fail_keys={"b"}, fail_times=1)
    assert producer.put_with_retries(k, "s", records("a", "b", "c")) == 0
    assert k.calls == [["a", "b", "c"], ["b"]]


def test_sin_fallas_un_solo_intento():
    k = FakeKinesis(fail_keys=set(), fail_times=0)
    assert producer.put_with_retries(k, "s", records("a", "b")) == 0
    assert len(k.calls) == 1


def test_respeta_max_attempts():
    k = FakeKinesis(fail_keys={"a"}, fail_times=99)
    assert producer.put_with_retries(k, "s", records("a", "b"), max_attempts=3) == 1
    assert len(k.calls) == 3


def test_backoff_exponencial(monkeypatch):
    delays = []
    monkeypatch.setattr(producer.time, "sleep", delays.append)
    k = FakeKinesis(fail_keys={"a"}, fail_times=99)
    producer.put_with_retries(k, "s", records("a"), max_attempts=4, base_delay=0.1)
    assert delays == pytest.approx([0.1, 0.2, 0.4])
