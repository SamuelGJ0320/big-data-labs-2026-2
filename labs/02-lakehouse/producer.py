"""
Productor de eventos de PayTrack en tiempo real: Kafka (local) o Kinesis (AWS).

Uso:
    # Local: ~20 eventos/s durante 2 minutos hacia Kafka
    python labs/02-lakehouse/producer.py --sink kafka --rate 20 --duration 120

    # Local, sin esperar: 10 minutos de eventos enviados de una vez (tiempo de evento simulado)
    python labs/02-lakehouse/producer.py --sink kafka --rate 20 --duration 600 --fast

    # AWS: hacia el stream de Kinesis del stack
    python labs/02-lakehouse/producer.py --sink kinesis --rate 20 --duration 240

Cada evento es un pago nuevo o un contracargo de un pago anterior (del lote 1 o
del mismo stream). Para que el streaming tenga los problemas de una fuente real,
el productor también:
  - reenvía eventos (mismo event_id): la fuente entrega al menos una vez
  - envía eventos tardíos: su event_time es varios minutos anterior a la llegada
  - inyecta ráfagas: 6 a 8 pagos del mismo cliente en pocos segundos (fraude)

Completa los TODO y valida tu lógica sin Kafka ni AWS con:
    uv run pytest labs/02-lakehouse/tests
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Bogota")
DUPLICATE_RATE = 0.03      # eventos reenviados
LATE_RATE = 0.03           # eventos que llegan tarde
LATE_DELAY = (30, 360)     # segundos de retraso de un evento tardío
CHARGEBACK_RATE = 0.02     # contracargos
BURST_RATE = 0.004         # probabilidad de iniciar una ráfaga de fraude
PAYMENT_METHODS = ["credit_card", "debit_card", "nequi", "daviplata", "pse"]


# ── TODO 1: partition key ────────────────────────────────────────────────────
# Kafka (particiones) y Kinesis (shards) reparten los eventos según el hash de
# su llave y solo garantizan el orden DENTRO de una partición. Elige la llave
# para que:
#   - todos los eventos de un mismo cliente (sus pagos, sus ráfagas y los
#     contracargos de sus pagos) lleguen a la misma partición, en orden
#   - la carga se reparta entre particiones (no uses un valor constante)
def partition_key(event: dict) -> str:
    raise NotImplementedError("TODO 1: partition_key")


# ── TODO 2: configuración del productor de Kafka ─────────────────────────────
# Devuelve la configuración de confluent_kafka.Producer para `bootstrap`, de
# modo que un evento confirmado no se pierda si falla el líder de la partición
# y que los reintentos automáticos del cliente no dupliquen ni reordenen
# eventos dentro de una partición.
# Pista: busca "acks" y "enable.idempotence" en la documentación de Kafka.
def kafka_config(bootstrap: str) -> dict:
    raise NotImplementedError("TODO 2: kafka_config")


# ── TODO 3: reintentos en Kinesis ────────────────────────────────────────────
# put_records NO es todo o nada: puede aceptar unos registros y rechazar otros
# (por ejemplo, si un shard supera su límite de escritura). La respuesta trae
# FailedRecordCount y, en "Records", un resultado por registro EN EL MISMO ORDEN
# del envío; los fallidos tienen la llave "ErrorCode".
#
# Envía `records` y reintenta SOLO los fallidos, hasta `max_attempts` intentos
# en total, esperando entre intentos con backoff exponencial
# (base_delay, 2*base_delay, 4*base_delay...). Devuelve cuántos registros
# quedaron sin enviar después del último intento.
def put_with_retries(kinesis, stream: str, records: list[dict],
                     max_attempts: int = 4, base_delay: float = 0.2) -> int:
    raise NotImplementedError("TODO 3: put_with_retries")


# ── Dado: generación de eventos ──────────────────────────────────────────────

def iso(ts: datetime) -> str:
    return ts.isoformat(timespec="milliseconds")


class EventSource:
    """Genera eventos de PayTrack a partir de los clientes y pagos del lote 1."""

    def __init__(self, batch_dir: Path, seed: int | None = None):
        self.rng = random.Random(seed)
        self.customers = [c["customer_id"] for c in json.loads((batch_dir / "customers.json").read_text())]
        self.merchants = json.loads((batch_dir / "merchants.json").read_text())
        valid = [t for t in json.loads((batch_dir / "transactions.json").read_text())
                 if t["status"] == "approved" and t["customer_id"]
                 and isinstance(t["amount_cop"], (int, float)) and 0 < t["amount_cop"] < 1e8]
        self.approved = list({t["transaction_id"]: t for t in valid}.values())  # pagos que se pueden contracargar
        self.sent: list[dict] = []  # para reenviar duplicados
        self.seq = 0

    def payment(self, now: datetime, customer: str | None = None, fraud: bool = False) -> dict:
        self.seq += 1
        merchant = self.rng.choice(self.merchants)
        status = "approved" if self.rng.random() > (0.3 if fraud else 0.05) else "declined"
        return {
            "transaction_id": f"TXNS{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12]}",
            "customer_id": customer or self.rng.choice(self.customers),
            "merchant_id": merchant["id"],
            "merchant_name": merchant["name"],
            "merchant_category": merchant["category"],
            "amount_cop": round(self.rng.lognormvariate(10.5, 1.2), 2),
            "payment_method": self.rng.choice(PAYMENT_METHODS),
            "status": status,
            "is_fraud": fraud,
            "timestamp": iso(now),
            "city": merchant["city"],
        }

    def envelope(self, txn: dict, event_type: str, event_time: datetime) -> dict:
        return {"event_id": str(uuid.UUID(int=self.rng.getrandbits(128))), "event_type": event_type,
                "event_time": iso(event_time), **txn}

    def next_events(self, now: datetime) -> list[dict]:
        """Eventos de un instante: normalmente uno, a veces un duplicado o una ráfaga."""
        r = self.rng.random()
        if r < DUPLICATE_RATE and self.sent:
            return [dict(self.rng.choice(self.sent[-200:]))]  # mismo event_id: reenvío
        if r < DUPLICATE_RATE + CHARGEBACK_RATE and self.approved:
            txn = self.approved.pop(self.rng.randrange(len(self.approved)))  # cada pago, máximo un contracargo
            return self._remember([self.envelope({**txn, "status": "chargeback"}, "chargeback", now)])
        if r < DUPLICATE_RATE + CHARGEBACK_RATE + BURST_RATE:
            customer = self.rng.choice(self.customers)
            burst = [self.payment(now + timedelta(seconds=i * 2), customer, fraud=True)
                     for i in range(self.rng.randint(6, 8))]
            return self._remember([self.envelope(t, "payment", datetime.fromisoformat(t["timestamp"]))
                                   for t in burst])
        event_time = now
        if self.rng.random() < LATE_RATE:
            event_time = now - timedelta(seconds=self.rng.randint(*LATE_DELAY))
        txn = self.payment(event_time)
        if txn["status"] == "approved":
            self.approved.append(txn)
        return self._remember([self.envelope(txn, "payment", event_time)])

    def _remember(self, events: list[dict]) -> list[dict]:
        self.sent += events
        return events


# ── Dado: destinos ───────────────────────────────────────────────────────────

class KafkaSink:
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer

        self.producer, self.topic, self.failed = Producer(kafka_config(bootstrap)), topic, 0
        try:
            self.producer.list_topics(timeout=10)
        except Exception as e:
            raise SystemExit(f"✗ No hay conexión con Kafka en {bootstrap}. ¿Está corriendo el contenedor? ({e})")

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1

    def send(self, events: list[dict]) -> int:
        for e in events:
            self.producer.produce(self.topic, key=partition_key(e), value=json.dumps(e, ensure_ascii=False),
                                  on_delivery=self._on_delivery)
        self.producer.poll(0)
        return 0

    def close(self) -> int:
        pending = self.producer.flush(30)  # eventos que no se alcanzaron a confirmar
        return self.failed + pending


class KinesisSink:
    def __init__(self, stream: str):
        import boto3

        self.kinesis, self.stream, self.failed = boto3.client("kinesis"), stream, 0

    def send(self, events: list[dict]) -> int:
        lost = 0
        for i in range(0, len(events), 500):  # put_records acepta hasta 500 registros
            records = [{"Data": json.dumps(e, ensure_ascii=False).encode("utf-8"), "PartitionKey": partition_key(e)}
                       for e in events[i:i + 500]]
            lost += put_with_retries(self.kinesis, self.stream, records)
        self.failed += lost
        return lost

    def close(self) -> int:
        return self.failed


def produce(sink, source: EventSource, rate: int, duration: int, fast: bool) -> None:
    # En modo --fast el reloj es simulado: los eventos cubren los últimos `duration`
    # segundos y se envían de una vez. Si no, se envían en tiempo real.
    clock = datetime.now(TZ) - timedelta(seconds=duration) if fast else None
    sent = 0
    counts: dict[str, int] = {}
    start = time.time()
    for second in range(duration):
        tick = time.time()
        now = clock + timedelta(seconds=second) if fast else datetime.now(TZ)
        events = [e for i in range(rate) for e in source.next_events(now + timedelta(milliseconds=i * 1000 // rate))]
        sink.send(events)
        sent += len(events)
        for e in events:
            counts[e["event_type"]] = counts.get(e["event_type"], 0) + 1
        if not fast:
            print(f"  enviados: {sent}", end="\r")
            time.sleep(max(0.0, 1 - (time.time() - tick)))
    by_type = ", ".join(f"{k}: {v}" for k, v in sorted(counts.items()))
    failed = sink.close()
    print(f"\n✓ {sent} eventos enviados en {time.time() - start:.0f}s ({by_type}) · {failed} perdidos")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sink", choices=["kafka", "kinesis"], required=True)
    parser.add_argument("--batch-dir", type=Path, default=Path("data/batch1"),
                        help="lote del que salen los clientes y los pagos a contracargar")
    parser.add_argument("--rate", type=int, default=20, help="eventos por segundo (aprox.)")
    parser.add_argument("--duration", type=int, default=120, help="segundos de eventos")
    parser.add_argument("--fast", action="store_true", help="enviar todo de una vez con tiempo simulado")
    parser.add_argument("--seed", type=int, help="semilla para reproducir el mismo stream")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="paytrack.transactions")
    parser.add_argument("--stream", default="paytrack-transactions", help="stream de Kinesis")
    args = parser.parse_args()

    sink = KafkaSink(args.bootstrap, args.topic) if args.sink == "kafka" else KinesisSink(args.stream)
    produce(sink, EventSource(args.batch_dir, args.seed), args.rate, args.duration, args.fast)
