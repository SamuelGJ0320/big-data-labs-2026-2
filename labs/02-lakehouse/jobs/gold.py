"""
Job Gold: silver → gold (Delta, una tabla por pregunta de negocio).

Las tablas Gold se recalculan completas desde Silver en cada ejecución y se
consultan desde Athena (ver sql/athena.sql).

Argumentos: --BASE <s3://bucket | carpeta local>
"""

from __future__ import annotations

from common import check, get_spark, parse_args, read_table, table_path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

args = parse_args(["BASE"])
BASE = args["BASE"].rstrip("/")
spark = get_spark("paytrack-gold")

txn = read_table(spark, table_path(BASE, "silver", "transactions"))
merchants = read_table(spark, table_path(BASE, "silver", "merchants"))
customers = read_table(spark, table_path(BASE, "silver", "customers"))
fraud = read_table(spark, table_path(BASE, "silver", "fraud_events"))


# ── TODO 1: daily_merchant_summary ───────────────────────────────────────────
# Una fila por DÍA y COMERCIO, para el dashboard de operaciones:
#   day                     fecha de la transacción (date)
#   merchant_id, merchant_name, merchant_category
#   num_transactions        número de transacciones (todos los estados)
#   approved_cop            suma de amount_cop de las aprobadas
#   chargeback_cop          suma de amount_cop de los contracargos (0 si no hay)
#   approval_rate_pct       % de transacciones aprobadas, 2 decimales
#   year, month             columnas de partición ("YYYY", "MM")
def daily_merchant_summary() -> DataFrame:
    return (
        txn.alias("t")
        .join(merchants.alias("m"), F.col("t.merchant_id") == F.col("m.merchant_id"), "left")
        .withColumn("day", F.to_date("t.event_ts"))
        .withColumn("year", F.date_format("t.event_ts", "yyyy"))
        .withColumn("month", F.date_format("t.event_ts", "MM"))
        .groupBy("day", "t.merchant_id", "m.merchant_name", "m.merchant_category", "year", "month")
        .agg(
            F.count("*").alias("num_transactions"),
            F.sum(F.when(F.col("t.status") == "approved", F.col("t.amount_cop"))).alias("approved_cop"),
            F.sum(F.when(F.col("t.status") == "chargeback", F.col("t.amount_cop")).otherwise(F.lit(0))).alias("chargeback_cop"),
            (F.sum(F.when(F.col("t.status") == "approved", F.lit(1)).otherwise(F.lit(0))) * 100.0 / F.count("*")).alias("approval_rate_pct_raw"),
        )
        .withColumn("approval_rate_pct", F.round("approval_rate_pct_raw", 2))
        .drop("approval_rate_pct_raw")
    )


# ── TODO 2: fraud_weekly_report ──────────────────────────────────────────────
# Una fila por SEMANA, PATRÓN de fraude y SEGMENTO de cliente:
#   week_start              lunes de la semana del evento (date)
#   pattern, segment
#   num_events              número de eventos de fraude
#   avg_risk_score          risk score promedio
#   total_cop               monto total de las transacciones involucradas
# Solo cuentan los eventos cuya transacción está en silver.transactions.
def fraud_weekly_report() -> DataFrame:
    return (
        fraud.alias("f")
        .join(txn.alias("t"), F.col("f.transaction_id") == F.col("t.transaction_id"), "inner")
        .join(customers.alias("c"), F.col("t.customer_id") == F.col("c.customer_id"), "left")
        .withColumn("week_start", F.trunc(F.col("t.event_ts"), "week").cast("date"))
        .groupBy("week_start", "f.pattern", "c.segment")
        .agg(
            F.count("*").alias("num_events"),
            F.avg("f.risk_score").alias("avg_risk_score"),
            F.sum("t.amount_cop").alias("total_cop"),
        )
    )


# ── TODO 3: hourly_volume ────────────────────────────────────────────────────
# Una fila por HORA del día (0-23) con las transacciones APROBADAS:
#   hour, num_transactions, total_cop, avg_cop (2 decimales)
def hourly_volume() -> DataFrame:
    return (
        txn.filter(F.col("status") == "approved")
        .withColumn("hour", F.hour("event_ts"))
        .groupBy("hour")
        .agg(
            F.count("*").alias("num_transactions"),
            F.sum("amount_cop").alias("total_cop"),
            F.round(F.avg("amount_cop"), 2).alias("avg_cop"),
        )
        .orderBy("hour")
    )


# ── Ejecución y validación (dadas) ───────────────────────────────────────────

def save(df: DataFrame, name: str, partition_by: list[str] | None = None) -> DataFrame:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(table_path(BASE, "gold", name))
    return read_table(spark, table_path(BASE, "gold", name))


daily = save(daily_merchant_summary(), "daily_merchant_summary", ["year", "month"])
weekly = save(fraud_weekly_report(), "fraud_weekly_report")
hourly = save(hourly_volume(), "hourly_volume")

total = lambda df, col: df.agg(F.sum(col)).first()[0]  # noqa: E731
approved = txn.filter("status = 'approved'")

check(daily.count() == daily.select("day", "merchant_id").distinct().count(), "daily: una fila por día y comercio")
check(total(daily, "num_transactions") == txn.count(), "daily: cuadra el número de transacciones con silver")
check(total(daily, "approved_cop") == total(approved, "amount_cop"), "daily: cuadra el monto aprobado")
check(total(daily, "chargeback_cop") == (total(txn.filter("status = 'chargeback'"), "amount_cop") or 0),
      "daily: cuadra el monto en contracargos")
check(daily.filter((F.length("year") != 4) | (F.length("month") != 2)).count() == 0, "daily: particiones bien formadas")
check(weekly.count() == weekly.select("week_start", "pattern", "segment").distinct().count(),
      "weekly: una fila por semana, patrón y segmento")
check(total(weekly, "num_events") == fraud.join(txn, "transaction_id").count(), "weekly: cuadra el número de eventos")
check(weekly.filter(F.dayofweek("week_start") != 2).count() == 0, "weekly: las semanas empiezan en lunes")
check(hourly.count() == 24, "hourly: 24 horas")
check(total(hourly, "num_transactions") == approved.count(), "hourly: cuadra con las aprobadas de silver")

print("\nGold listo")
