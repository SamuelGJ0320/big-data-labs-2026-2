#!/usr/bin/env bash
# ================================================================
# Ejecuta el pipeline en AWS (desde la raíz del repositorio, con el stack ya desplegado).
#
# Batch: sube los jobs, ingesta el lote en S3 y corre los 4 jobs de Glue en orden.
#   bash labs/02-lakehouse/run_aws.sh <carpeta-del-lote> <ingest-date>
#   bash labs/02-lakehouse/run_aws.sh data/batch1 2026-09-29
#
# Streaming: lanza el job de Glue Streaming y envía eventos a Kinesis mientras corre.
#   bash labs/02-lakehouse/run_aws.sh stream [segundos]     # por defecto 300
# ================================================================
set -euo pipefail

LAB=labs/02-lakehouse

BUCKET=$(aws cloudformation describe-stacks --stack-name paytrack-lakehouse-dev \
  --query "Stacks[0].Outputs[?OutputKey=='LakehouseBucket'].OutputValue" --output text)
echo "Bucket: $BUCKET"

echo "━━━ subiendo jobs ━━━"
aws s3 cp "$LAB/jobs/" "s3://$BUCKET/scripts/" --recursive --exclude "*" --include "*.py" --quiet

start_job() {  # start_job <job> <argumentos JSON> → imprime el id de la ejecución
  aws glue start-job-run --job-name "paytrack-$1" --arguments "$2" --query JobRunId --output text
}

wait_job() {  # wait_job <job> <run-id>: espera a que termine; si falla, muestra el error
  local job=$1 run_id=$2 state error
  while true; do
    read -r state error < <(aws glue get-job-run --job-name "paytrack-$job" --run-id "$run_id" \
      --query "JobRun.[JobRunState,ErrorMessage]" --output text)
    case "$state" in
      STARTING|RUNNING|WAITING|STOPPING) printf "."; sleep 15 ;;
      SUCCEEDED) echo " ✓ $state"; break ;;
      *)
        echo -e " ✗ $state\n$error"
        echo "Revisa los logs en CloudWatch: /aws-glue/jobs/error y /aws-glue/jobs/output (run $run_id)"
        exit 1 ;;
    esac
  done
}

if [[ ${1:-} == stream ]]; then
  RUN_SECONDS=${2:-300}
  echo -e "\n━━━ stream ━━━"
  run_id=$(start_job stream "{\"--RUN_SECONDS\":\"$RUN_SECONDS\"}")
  echo "Job paytrack-stream lanzado ($run_id). Glue tarda ~1 min en arrancar; Kinesis guarda"
  echo "los eventos mientras tanto y el job los lee desde el más antiguo."
  # El productor termina un minuto antes que el job para que alcance a procesarlo todo
  uv run python "$LAB/producer.py" --sink kinesis --rate 20 --duration $((RUN_SECONDS - 60))
  echo "Esperando a que el job termine (corre $RUN_SECONDS s y luego valida)"
  wait_job stream "$run_id"
  exit
fi

BATCH_DIR=${1:?Falta la carpeta del lote, ej. data/batch1}
INGEST_DATE=${2:?Falta la fecha de ingesta, ej. 2026-09-30}

echo "━━━ ingest ━━━"
uv run python "$LAB/ingest.py" --batch-dir "$BATCH_DIR" --ingest-date "$INGEST_DATE" --base "s3://$BUCKET"

for job in bronze silver gold audit; do
  echo -e "\n━━━ $job ━━━"
  wait_job "$job" "$(start_job "$job" "{\"--INGEST_DATE\":\"$INGEST_DATE\"}")"
done

echo -e "\nPipeline completo para el lote $INGEST_DATE"
