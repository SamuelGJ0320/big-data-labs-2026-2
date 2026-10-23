"""
Job Stream: eventos en tiempo real → lakehouse, con Spark Structured Streaming.

    Kafka (local) / Kinesis (AWS)
        │  consulta bronze
        ▼
    bronze/transaction_events        Delta, append: cada evento tal como llegó
        │  (la tabla Delta es a su vez la fuente de las siguientes consultas)
        ├── consulta silver  ──→  silver/transactions          MERGE por micro-lote (la MISMA tabla del batch)
        ├── consulta metrics ──→  gold/merchant_minute_metrics  ventanas de 1 minuto por comercio
        └── consulta alerts  ──→  gold/fraud_alerts             ráfagas de pagos por cliente

Cada consulta guarda su progreso en un checkpoint (BASE/checkpoints/<consulta>):
si el job se detiene y se vuelve a lanzar, continúa donde iba sin perder ni
repetir eventos en sus tablas.

Requisito: silver.transactions ya existe (corre antes el pipeline batch del lote 1).

Argumentos:
  --BASE <s3://bucket | carpeta local>
  --SOURCE kafka | kinesis
  --STREAM <topic de Kafka | ARN del stream de Kinesis>
  --BOOTSTRAP <broker de Kafka | none>
  --RUN_SECONDS <0: procesa lo disponible y termina | N: corre en vivo N segundos>
"""

from __future__ import annotations

import time

from common import check, get_spark, parse_args, read_events, read_table, table_exists, table_path
from delta.tables import DeltaTable  # noqa: F401 (la usarás en un TODO)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window  # noqa: F401 (la usarás en un TODO)

WATERMARK = "2 minutes"      # cuánto retraso se tolera en event_time
METRICS_WINDOW = "1 minute"  # ventanas fijas (tumbling) de las métricas por comercio
ALERT_WINDOW, ALERT_SLIDE = "30 seconds", "10 seconds"  # ventanas deslizantes de las alertas
ALERT_MIN_PAYMENTS = 6       # 6 o más pagos de un cliente en una ventana → alerta

args = parse_args(["BASE", "SOURCE", "STREAM", "BOOTSTRAP", "RUN_SECONDS"])
BASE, RUN_SECONDS = args["BASE"].rstrip("/"), int(args["RUN_SECONDS"])
spark = get_spark("paytrack-stream")
spark.conf.set("spark.sql.shuffle.partitions", 4)  # volumen pequeño: pocas particiones de estado

BRONZE = table_path(BASE, "bronze", "transaction_events")
SILVER = table_path(BASE, "silver", "transactions")
METRICS = table_path(BASE, "gold", "merchant_minute_metrics")
ALERTS = table_path(BASE, "gold", "fraud_alerts")


def checkpoint(query: str) -> str:
    return f"{BASE}/checkpoints/{query}"


# ── TODO 1: esquema de los eventos ───────────────────────────────────────────
# En streaming el esquema se declara: no se puede inferir de datos que todavía
# no han llegado. Devuelve el esquema de los eventos de producer.py en formato
# DDL ("col1 STRING, col2 DOUBLE, ..."), con TODOS sus campos:
#   - event_time y timestamp como STRING (se convierten a timestamp después)
#   - amount_cop como DOUBLE e is_fraud como BOOLEAN
#   - "timestamp" es palabra reservada: escríbela entre comillas invertidas (`timestamp`)
# Si un campo falta o tiene un tipo que no corresponde, llega como NULL.
def event_schema() -> str:
    raise NotImplementedError("TODO 1: event_schema")


# ── TODO 2: deduplicar con watermark ─────────────────────────────────────────
# La fuente entrega cada evento AL MENOS una vez: algunos llegan repetidos (mismo
# event_id). Devuelve `events` sin repetidos, usando event_time como tiempo del
# evento con un watermark de WATERMARK.
#   - withWatermark le dice a Spark cuánto esperar eventos tardíos; así puede
#     olvidar el estado viejo (si no, la memoria crecería sin límite)
#   - dropDuplicates en streaming solo puede limpiar su estado si la columna del
#     watermark (event_time) hace parte de la llave. Un reenvío trae el mismo
#     event_id Y el mismo event_time.
def deduplicate_events(events: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO 2: deduplicate_events")


# ── TODO 3: un evento por transacción ────────────────────────────────────────
# Un micro-lote puede traer varios eventos de la misma transacción (el pago y
# su contracargo, o un reenvío), pero MERGE exige a lo sumo una fila de origen
# por llave. Deja una sola fila por transaction_id: la de event_time más
# reciente. Recibe un DataFrame estático (un micro-lote), no un stream.
def latest_per_transaction(batch: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO 3: latest_per_transaction")


# ── TODO 4: upsert de un micro-lote en silver ────────────────────────────────
# foreachBatch llama esta función con cada micro-lote de bronze. Integra `rows`
# (ya con una fila por transaction_id y las columnas de silver.transactions) en
# la tabla Delta SILVER con MERGE:
#   - la llave es transaction_id
#   - si ya existe y su status cambió: actualiza status, is_fraud y silver_updated_at
#   - pero 'chargeback' es un estado final: un reenvío o un evento tardío de la
#     misma transacción nunca lo puede revertir
#   - si no existe: insértala completa
# Si Spark re-ejecuta un micro-lote tras una falla, el resultado debe ser el mismo.
def merge_into_silver(rows: DataFrame) -> None:
    raise NotImplementedError("TODO 4: merge_into_silver")


# ── TODO 5: métricas por minuto y comercio ───────────────────────────────────
# A partir de los eventos sin repetidos, solo los de event_type = 'payment',
# una fila por ventana fija de METRICS_WINDOW (sobre event_time) y comercio:
#   window_start, window_end   inicio y fin de la ventana
#   merchant_id, merchant_category
#   num_payments               número de pagos
#   approved_cop               suma de amount_cop de los aprobados (0 si no hay)
#   approval_rate_pct          % de pagos aprobados, 2 decimales
# Pista: F.window. La tabla se escribe en modo append: una ventana se emite una
# sola vez, cuando el watermark la cierra.
def merchant_metrics(clean: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO 5: merchant_metrics")


# ── TODO 6: alertas de ráfagas ───────────────────────────────────────────────
# Detecta clientes con ALERT_MIN_PAYMENTS o más pagos (event_type = 'payment')
# dentro de una ventana deslizante de ALERT_WINDOW que avanza cada ALERT_SLIDE.
# Una fila por cliente y ventana con alerta:
#   window_start, window_end, customer_id, num_payments, total_cop
def fraud_alerts(clean: DataFrame) -> DataFrame:
    raise NotImplementedError("TODO 6: fraud_alerts")


# ── Dado: transformaciones fijas ─────────────────────────────────────────────

def to_bronze(parsed: DataFrame) -> DataFrame:
    """Tipos de tiempo y monto, y metadatos de llegada. Sin filtrar nada."""
    return (
        parsed.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .drop("timestamp")
        .withColumn("amount_cop", F.col("amount_cop").cast("decimal(15,2)"))
        .withColumn("ingested_at", F.current_timestamp())
    )


def to_silver(rows: DataFrame) -> DataFrame:
    """Lleva los eventos a las columnas de silver.transactions (las que creó el batch)."""
    columns = read_table(spark, SILVER).columns
    rows = (
        rows.withColumn("ingest_date", F.date_format("arrival_ts", "yyyy-MM-dd"))
        .withColumn("year", F.date_format("event_ts", "yyyy"))
        .withColumn("month", F.date_format("event_ts", "MM"))
        .withColumn("silver_updated_at", F.current_timestamp())
    )
    missing = set(columns) - set(rows.columns)
    if missing:
        raise AssertionError(f"✗ a los eventos les faltan columnas de silver.transactions: {sorted(missing)}")
    return rows.select(*columns)


def upsert_micro_batch(batch: DataFrame, batch_id: int) -> None:
    merge_into_silver(to_silver(latest_per_transaction(batch)))


# ── Dado: consultas de streaming ─────────────────────────────────────────────

def start_bronze(trigger: dict):
    events = to_bronze(read_events(spark, args["SOURCE"], args["STREAM"], args["BOOTSTRAP"], event_schema()))
    return (events.writeStream.format("delta").outputMode("append").queryName("bronze")
            .option("checkpointLocation", checkpoint("bronze")).trigger(**trigger).start(BRONZE))


def start_downstream(trigger: dict) -> list:
    events = spark.readStream.format("delta").option("maxFilesPerTrigger", 6).load(BRONZE)
    clean = deduplicate_events(events)
    silver = (events.writeStream.queryName("silver").foreachBatch(upsert_micro_batch)
              .option("checkpointLocation", checkpoint("silver")).trigger(**trigger).start())
    metrics = (merchant_metrics(clean).writeStream.format("delta").outputMode("append").queryName("metrics")
               .option("checkpointLocation", checkpoint("metrics")).trigger(**trigger).start(METRICS))
    alerts = (fraud_alerts(clean).writeStream.format("delta").outputMode("append").queryName("alerts")
              .option("checkpointLocation", checkpoint("alerts")).trigger(**trigger).start(ALERTS))
    return [silver, metrics, alerts]


def report(queries: list) -> dict:
    """Filas procesadas y filas descartadas por llegar después del watermark, por consulta."""
    stats = {}
    for q in queries:
        rows = sum(p["numInputRows"] for p in q.recentProgress)
        dropped = sum(op.get("numRowsDroppedByWatermark", 0) for p in q.recentProgress
                      for op in p.get("stateOperators", []))
        stats[q.name] = stats.get(q.name, 0) + dropped
        print(f"  {q.name:8s} micro-lotes: {len(q.recentProgress):3d} · filas leídas: {rows:6d} · "
              f"descartadas por watermark: {dropped}")
    return stats


check(table_exists(spark, SILVER), "silver.transactions existe (corre antes el batch del lote 1)")

if RUN_SECONDS == 0:
    # Procesa todo lo disponible y termina: bronze primero, luego lo que sale de bronze
    print("Modo availableNow: se procesa lo que hay en el broker y el job termina")
    bronze = start_bronze({"availableNow": True})
    bronze.awaitTermination()
    report([bronze])
    downstream = start_downstream({"availableNow": True})
    for q in downstream:
        q.awaitTermination()
    report(downstream)
else:
    # En vivo: todas las consultas corren a la vez, con un micro-lote cada 10 s
    print(f"Modo en vivo: micro-lotes cada 10 s durante {RUN_SECONDS} s")
    bronze = start_bronze({"processingTime": "10 seconds"})
    while not table_exists(spark, BRONZE):
        check(bronze.exception() is None, "la consulta bronze está corriendo")
        time.sleep(5)
    downstream = start_downstream({"processingTime": "10 seconds"})
    spark.streams.awaitAnyTermination(RUN_SECONDS)  # lanza error si una consulta falló
    for q in [bronze, *downstream]:
        q.stop()
    report([bronze, *downstream])
    # Al apagar: termina de procesar lo que ya está en bronze para dejar todo consistente
    downstream = start_downstream({"availableNow": True})
    for q in downstream:
        q.awaitTermination()
    report(downstream)


# ── Validación (dada) ────────────────────────────────────────────────────────

bronze_df = read_table(spark, BRONZE)
silver_df = read_table(spark, SILVER)
metrics_df = read_table(spark, METRICS)
alerts_df = read_table(spark, ALERTS)
payments = bronze_df.filter("event_type = 'payment'")
seconds = lambda df: F.col("window_end").cast("long") - F.col("window_start").cast("long")  # noqa: E731

check(bronze_df.count() > 0, f"bronze.transaction_events: {bronze_df.count()} eventos")
check(bronze_df.filter("event_id IS NULL OR event_time IS NULL OR event_ts IS NULL OR transaction_id IS NULL "
                       "OR amount_cop IS NULL OR is_fraud IS NULL OR status IS NULL").count() == 0,
      "bronze: el esquema parsea todos los campos (sin NULL inesperados)")
check(deduplicate_events(bronze_df).count() == bronze_df.select("event_id").distinct().count(),
      "deduplicate_events: deja un evento por event_id")

expected = latest_per_transaction(bronze_df).select("transaction_id", "status")
check(expected.count() == bronze_df.select("transaction_id").distinct().count(),
      "latest_per_transaction: una fila por transaction_id")
check(silver_df.count() == silver_df.select("transaction_id").distinct().count(),
      "silver: sin transaction_id duplicados")
in_silver = expected.alias("e").join(silver_df.alias("s"), "transaction_id")
check(in_silver.count() == expected.count(), "silver: llegaron todas las transacciones del stream (también las tardías)")
check(in_silver.filter(F.col("e.status") != F.col("s.status")).count() == 0,
      "silver: cada transacción quedó con el status de su último evento")
check(bronze_df.filter("event_type = 'chargeback'").join(silver_df, "transaction_id")
      .filter(silver_df.status != "chargeback").count() == 0,
      "silver: ningún contracargo fue revertido por un reenvío")

check(metrics_df.count() > 0, f"metrics: {metrics_df.count()} ventanas emitidas")
check(metrics_df.count() == metrics_df.select("window_start", "merchant_id").distinct().count(),
      "metrics: una fila por ventana y comercio")
check(metrics_df.filter(seconds(metrics_df) != 60).count() == 0, "metrics: ventanas de 1 minuto")
check(metrics_df.agg(F.sum("num_payments")).first()[0] <= payments.select("event_id").distinct().count(),
      "metrics: no cuenta pagos repetidos")
check(metrics_df.filter("approval_rate_pct < 0 OR approval_rate_pct > 100 OR approved_cop < 0").count() == 0,
      "metrics: tasas y montos en rango")

check(alerts_df.count() == alerts_df.select("window_start", "customer_id").distinct().count(),
      "alerts: una fila por ventana y cliente")
check(alerts_df.filter(f"num_payments < {ALERT_MIN_PAYMENTS}").count() == 0,
      f"alerts: todas tienen {ALERT_MIN_PAYMENTS} o más pagos")
check(alerts_df.filter(seconds(alerts_df) != 30).count() == 0, "alerts: ventanas de 30 segundos")

print(f"\nStream listo: {silver_df.count()} transacciones en silver, {metrics_df.count()} ventanas de métricas, "
      f"{alerts_df.count()} alertas")
