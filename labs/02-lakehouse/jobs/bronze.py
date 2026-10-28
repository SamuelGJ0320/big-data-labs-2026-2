"""
Job Bronze: raw (JSON Lines) → bronze (Delta).

Bronze guarda cada lote tal como llegó, más metadatos de ingesta. Todas las
columnas se leen como texto: el tipado es trabajo de Silver, y así un lote con
un tipo distinto no rompe la tabla.

Argumentos: --BASE <s3://bucket | carpeta local> --INGEST_DATE <YYYY-MM-DD>
"""

from __future__ import annotations

from common import check, get_spark, parse_args, read_table, table_path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DATASETS = ["transactions", "fraud_events", "customers", "merchants"]

args = parse_args(["BASE", "INGEST_DATE"])
BASE, INGEST_DATE = args["BASE"].rstrip("/"), args["INGEST_DATE"]
spark = get_spark("paytrack-bronze")


def read_raw(dataset: str) -> DataFrame:
    """Dado: lee el lote de raw con todas las columnas como texto."""
    return spark.read.option("primitivesAsString", True).json(
        f"{BASE}/raw/{dataset}/ingest_date={INGEST_DATE}/"
    )


# ── TODO 1: metadatos de ingesta ─────────────────────────────────────────────
# Agrega tres columnas de auditoría:
#   - ingest_date: el valor de INGEST_DATE (texto)
#   - ingested_at: la marca de tiempo actual
#   - source_file: la ruta del archivo de origen (columna oculta _metadata.file_path)
def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("ingest_date", F.lit(INGEST_DATE))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name()))


# ── TODO 2: escritura idempotente ────────────────────────────────────────────
# Escribe df en la tabla Delta de `path`, particionada por ingest_date.
# Requisito: si el job se ejecuta dos veces con el mismo INGEST_DATE, la tabla
# debe quedar igual que tras una sola ejecución (sin duplicar el lote), y los
# lotes de otras fechas no se deben tocar.
# Pista: busca la opción replaceWhere de Delta Lake.
def write_bronze(df: DataFrame, path: str) -> None:
    (df.write.format("delta")
        .mode("overwrite")
        .partitionBy("ingest_date")
        .option("replaceWhere", f"ingest_date = '{INGEST_DATE}'")
        .save(path))


# ── Ejecución y validación (dadas) ───────────────────────────────────────────

for dataset in DATASETS:
    raw = read_raw(dataset)
    path = table_path(BASE, "bronze", dataset)
    write_bronze(add_ingestion_metadata(raw), path)

    bronze = read_table(spark, path)
    batch = bronze.filter(F.col("ingest_date") == INGEST_DATE)
    check({"ingest_date", "ingested_at", "source_file"} <= set(bronze.columns),
          f"{dataset}: tiene las columnas de auditoría")
    check(batch.count() == raw.count(), f"{dataset}: el lote {INGEST_DATE} tiene {raw.count()} filas (sin duplicar)")
    check(batch.filter(~F.col("source_file").contains(f"ingest_date={INGEST_DATE}")).count() == 0,
          f"{dataset}: source_file apunta al archivo del lote")

print(f"\nBronze listo para el lote {INGEST_DATE}")
