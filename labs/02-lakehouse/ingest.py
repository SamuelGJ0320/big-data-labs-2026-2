"""
Ingesta: copia un lote de data/<lote>/*.json a la zona raw del lakehouse.

Uso:
    python labs/02-lakehouse/ingest.py --batch-dir data/batch1 --ingest-date 2026-09-30 \\
        --base lakehouse-local            # en local
        --base s3://<tu-bucket>           # en AWS

La zona raw guarda cada lote tal como llegó, separado por fecha de llegada
(ingest_date), para que los jobs procesen un lote a la vez y se puedan re-ejecutar.

Completa los TODO y valida tu lógica con:
    uv run pytest labs/02-lakehouse/tests
"""

import argparse
import json
from pathlib import Path
from datetime import date

DATASETS = ["transactions", "fraud_events", "customers", "merchants"]


# ── TODO 1: prefijo de la partición de llegada ───────────────────────────────
# Devuelve el prefijo donde queda un dataset para un lote, con partición estilo
# Hive para que Spark, Glue y Athena la reconozcan:
#   raw/<dataset>/ingest_date=<YYYY-MM-DD>/
# Ejemplo: raw_prefix("transactions", "2026-09-30") -> "raw/transactions/ingest_date=2026-09-30/"
# Si la fecha no es una fecha válida en formato YYYY-MM-DD, lanza ValueError.
def raw_prefix(dataset: str, ingest_date: str) -> str:
    try:
        # validación estricta: solo YYYY-MM-DD con fecha real
        if date.fromisoformat(ingest_date).isoformat() != ingest_date:
            raise ValueError
    except ValueError:
        raise ValueError(f"ingest_date inválida: {ingest_date!r} (se espera YYYY-MM-DD)")
    return f"raw/{dataset}/ingest_date={ingest_date}/"


# ── TODO 2: formato JSON Lines ───────────────────────────────────────────────
# generate_data.py produce un arreglo JSON ([{...}, {...}]), pero Spark, Glue y
# Athena esperan JSON Lines: un objeto JSON por línea, sin corchetes ni comas
# entre registros. Convierte la lista a ese formato, conservando las tildes.
def to_json_lines(records: list[dict]) -> str:
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in records)


# ── Dado ─────────────────────────────────────────────────────────────────────

def write(base: str, key: str, body: str) -> None:
    if base.startswith("s3://"):
        import boto3

        bucket = base.removeprefix("s3://").split("/")[0]
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    else:
        path = Path(base) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")


def ingest(batch_dir: Path, ingest_date: str, base: str) -> None:
    for dataset in DATASETS:
        records = json.loads((batch_dir / f"{dataset}.json").read_text())
        key = f"{raw_prefix(dataset, ingest_date)}part-0000.json"
        write(base.rstrip("/"), key, to_json_lines(records))
        print(f"OK {dataset}: {len(records)} registros -> {base.rstrip('/')}/{key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-dir", type=Path, required=True)
    parser.add_argument("--ingest-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--base", required=True, help="carpeta local o s3://bucket")
    args = parser.parse_args()
    ingest(args.batch_dir, args.ingest_date, args.base)
