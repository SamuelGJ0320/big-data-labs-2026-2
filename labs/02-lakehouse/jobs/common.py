"""
Utilidades compartidas por los jobs del Lab 2 (dadas, no las modifiques).

Los mismos jobs corren en dos lugares:
  - AWS Glue 5.0: BASE = s3://<bucket>
  - Local (Docker + Spark): BASE = /workspace/lakehouse-local
"""

from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession


def parse_args(names: list[str]) -> dict:
    """Lee --NOMBRE valor. En Glue usa getResolvedOptions; en local, argparse."""
    try:
        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(sys.argv, names)
    except ImportError:
        import argparse

        parser = argparse.ArgumentParser()
        for name in names:
            parser.add_argument(f"--{name}", required=True)
        return vars(parser.parse_known_args()[0])


def get_spark(app: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "America/Bogota")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def table_path(base: str, layer: str, name: str) -> str:
    """Ruta de una tabla Delta: <base>/lakehouse/<capa>/<tabla>"""
    return f"{base}/lakehouse/{layer}/{name}"


def table_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def read_table(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def read_events(spark: SparkSession, source: str, stream: str, bootstrap: str, schema: str) -> DataFrame:
    """Lee el stream de eventos como un DataFrame de streaming, parseado con `schema` (DDL).

    - kafka   (local): `stream` es el topic y `bootstrap` el broker
    - kinesis (Glue):  `stream` es el ARN del stream; lo lee el conector de Glue

    Devuelve las columnas del esquema más arrival_ts: cuándo llegó el evento al broker.
    Empieza desde el evento más antiguo que retiene el broker; después, el checkpoint
    de cada consulta recuerda hasta dónde se leyó.
    """
    from pyspark.sql import functions as F

    if source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", stream)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 2000)  # micro-lotes pequeños, como llegarían en vivo
            .load()
        )
        return raw.select(
            F.from_json(F.col("value").cast("string"), schema).alias("e"), F.col("timestamp").alias("arrival_ts")
        ).select("e.*", "arrival_ts")

    from awsglue.context import GlueContext

    df = GlueContext(spark.sparkContext).create_data_frame.from_options(
        connection_type="kinesis",
        connection_options={
            "streamARN": stream,
            "startingPosition": "TRIM_HORIZON",
            "classification": "json",
            "inferSchema": "false",
            "schema": schema,
            "addRecordTimestamp": "true",
            "maxFetchRecordsPerShard": "2000",
        },
    )
    src = "__src_timestamp"
    arrival = F.timestamp_millis(src) if dict(df.dtypes)[src] in ("bigint", "int") else F.col(src).cast("timestamp")
    return df.withColumn("arrival_ts", arrival).drop(src)


def check(condition: bool, message: str) -> None:
    """Autoevaluación: si falla, el job termina con error y muestra el motivo."""
    if not condition:
        raise AssertionError(f"✗ {message}")
    print(f"✓ {message}")
