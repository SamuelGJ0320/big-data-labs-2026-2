#!/usr/bin/env bash
# ================================================================
# Ejecuta el pipeline de un lote en tu computador (Docker + Spark + Delta Lake).
# Los datos quedan en ./lakehouse-local (no se sube a git).
#
# Uso (desde la raíz del repositorio):
#   bash labs/02-lakehouse/run_local.sh <carpeta-del-lote> <ingest-date> [paso]
#   bash labs/02-lakehouse/run_local.sh data/batch1 2026-09-30            # todo
#   bash labs/02-lakehouse/run_local.sh data/batch1 2026-09-30 silver     # solo un paso
# Pasos: ingest | bronze | silver | gold | audit | all
#
# Streaming (Kafka debe estar corriendo, ver README):
#   bash labs/02-lakehouse/run_local.sh stream          # procesa lo disponible y termina
#   bash labs/02-lakehouse/run_local.sh stream 180      # en vivo durante 180 s
# ================================================================
set -euo pipefail

LAB=labs/02-lakehouse
BASE=lakehouse-local
SPARK_IMAGE=apache/spark:3.5.3-python3
PACKAGES=io.delta:delta-spark_2.12:3.2.1
NETWORK=bridge
IVY_CACHE="$HOME/.cache/paytrack-ivy"
mkdir -p "$IVY_CACHE" "$BASE"

# Oculta los logs de arranque de Spark y de descarga de dependencias
quiet() {
  awk '!/^[0-9][0-9]\/[0-9][0-9]\/[0-9][0-9] [0-9:]+ (INFO|WARN)/ && !/^(::|Ivy|The jars|io\.delta#|[ \t]+(::|io\.delta#|org\.antlr#|[0-9]+ artifacts|confs|found|downloading|\[SUCCESSFUL|\||-----))/'
}

run_job() {
  local job=$1
  shift
  echo -e "\n━━━ $job ━━━"
  # Spark corre como root dentro del contenedor; al terminar, los archivos
  # creados se devuelven a tu usuario para que puedas borrarlos sin sudo.
  docker run --rm --network "$NETWORK" -e HOME=/tmp -v "$PWD:/workspace" -v "$IVY_CACHE:/tmp/.ivy" -w /workspace \
    --user 0 "$SPARK_IMAGE" bash -c '
      /opt/spark/bin/spark-submit \
        --packages "$0" --conf spark.jars.ivy=/tmp/.ivy --conf spark.ui.enabled=false \
        --py-files "$1/jobs/common.py" "$1/jobs/$2.py" --BASE "/workspace/$3" "${@:5}"
      rc=$?
      chown -R "$4" "/workspace/$3" /tmp/.ivy
      exit $rc
    ' "$PACKAGES" "$LAB" "$job" "$BASE" "$(id -u):$(id -g)" "$@" 2>&1 | quiet
}

if [[ ${1:-} == stream ]]; then
  docker network inspect paytrack-streaming >/dev/null 2>&1 || {
    echo "Kafka no está corriendo: docker compose -f .docker/docker-compose.yml up -d kafka" >&2; exit 1; }
  PACKAGES=$PACKAGES,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3
  NETWORK=paytrack-streaming
  run_job stream --SOURCE kafka --STREAM paytrack.transactions --BOOTSTRAP kafka:29092 --RUN_SECONDS "${2:-0}"
  exit
fi

BATCH_DIR=${1:?Falta la carpeta del lote, ej. data/batch1}
INGEST_DATE=${2:?Falta la fecha de ingesta, ej. 2026-09-30}
STEP=${3:-all}

run_ingest() {
  echo "━━━ ingest ━━━"
  uv run python "$LAB/ingest.py" --batch-dir "$BATCH_DIR" --ingest-date "$INGEST_DATE" --base "$BASE"
}

case "$STEP" in
  ingest) run_ingest ;;
  bronze) run_job bronze --INGEST_DATE "$INGEST_DATE" ;;
  silver) run_job silver --INGEST_DATE "$INGEST_DATE" ;;
  gold)   run_job gold ;;
  audit)  run_job audit ;;
  all)
    run_ingest
    run_job bronze --INGEST_DATE "$INGEST_DATE"
    run_job silver --INGEST_DATE "$INGEST_DATE"
    run_job gold
    run_job audit
    ;;
  *) echo "Paso desconocido: $STEP" >&2; exit 1 ;;
esac
