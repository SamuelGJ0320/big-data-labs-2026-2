# Lab 2 · Lakehouse end-to-end en AWS: batch y streaming

**Entrega:** viernes 6 de noviembre de 2026 · **Peso:** 30%

## Objetivo

Construir el lakehouse de PayTrack de punta a punta, con las dos formas en que llegan los datos:

- **Parte A · Batch.** Ingestar lotes diarios en S3, procesarlos con la arquitectura Medallion (Bronze → Silver → Gold) en tablas **Delta Lake** con **AWS Glue**, auditar los cambios con **time travel** y consultar con **Amazon Athena**.
- **Parte B · Streaming.** Recibir los pagos en tiempo real por **Kafka** (local) o **Amazon Kinesis** (AWS), procesarlos con **Spark Structured Streaming** e integrarlos en **las mismas tablas Delta** del batch, con métricas y alertas de fraude por ventanas de tiempo.

Es la arquitectura que vimos en clase: *S3 + Delta Lake + Glue + Athena*, con una capa de streaming que escribe en el mismo lakehouse.

## Arquitectura

```
                PARTE A · BATCH                                  PARTE B · STREAMING

data/batchN/*.json                                    producer.py  (pagos, contracargos, reenvíos,
      │  ingest.py                                        │         eventos tardíos, ráfagas de fraude)
      ▼                                                   ▼
S3 raw/<dataset>/ingest_date=YYYY-MM-DD/          Kafka topic paytrack.transactions   (local)
      │  job bronze (Glue)                         Kinesis stream paytrack-transactions (AWS)
      ▼                                                   │  job stream (Spark Structured Streaming)
lakehouse/bronze/<dataset>                                ▼
      │  job silver (Glue)                         lakehouse/bronze/transaction_events   append + checkpoint
      ▼                                                   │
lakehouse/silver/transactions  ◄──── MERGE por micro-lote ┤
lakehouse/silver/transactions_quarantine                  ├──► lakehouse/gold/merchant_minute_metrics
lakehouse/silver/customers (PII enmascarada)              │      ventanas de 1 min + watermark
      │  job gold (Glue)        job audit (time travel)   └──► lakehouse/gold/fraud_alerts
      ▼                               ▼                          ventanas deslizantes por cliente
lakehouse/gold/daily_merchant_summary   lakehouse/gold/status_changes
lakehouse/gold/fraud_weekly_report
lakehouse/gold/hourly_volume
      │
      ▼
Amazon Athena (sql/athena.sql) ◄──────────────────── consulta las tablas de las dos partes
```

`silver.transactions` es la misma tabla para las dos partes: los pagos del stream quedan disponibles para Gold, para la auditoría y para Athena igual que los del batch.

## Qué está dado y qué construyes tú

| Archivo | Parte | Estado | Qué hace |
|---------|-------|--------|----------|
| `ingest.py` | A | ✏️ **TODO 1–2** | Partición de llegada y formato JSON Lines |
| `jobs/bronze.py` | A | ✏️ **TODO 1–2** | Metadatos de ingesta y escritura idempotente (`replaceWhere`) |
| `jobs/silver.py` | A | ✏️ **TODO 1–5** | Deduplicación, tipado, reglas de calidad, `MERGE` y enmascaramiento de PII |
| `jobs/gold.py` | A | ✏️ **TODO 1–3** | Tres tablas de negocio |
| `jobs/audit.py` | A | ✏️ **TODO 1–2** | Time travel: qué transacciones cambiaron con el último lote |
| `producer.py` | B | ✏️ **TODO 1–3** | Partition key, configuración de Kafka y reintentos en Kinesis |
| `jobs/stream.py` | B | ✏️ **TODO 1–6** | Esquema, deduplicación con watermark, `MERGE` por micro-lote, ventanas |
| `sql/athena.sql` | A y B | ✏️ **TODO 1–6** | Consultas de negocio sobre las tablas Delta |
| `infra/serverless.yml` | A y B | ✏️ **TODO** | Todo dado salvo el ciclo de vida de la zona raw |
| `jobs/common.py`, `run_local.sh`, `run_aws.sh`, `tests/` | A y B | ✅ Dado | Utilidades, ejecución y autoevaluación |

Cada job termina con **validaciones** (`✓` / `✗`) que no debes modificar: si una falla, el job termina con error y te dice qué revisar.

---

# Parte A · Batch

## El escenario: dos lotes

PayTrack recibe datos en lotes diarios con problemas reales de calidad: duplicados, montos inválidos, clientes faltantes y números que llegan como texto.

- **Lote 1** (`ingest_date = 2026-09-29`): 60 días de historia.
- **Lote 2** (`ingest_date = 2026-09-30`): transacciones nuevas, **contracargos** de transacciones del lote 1 (cambian a `status = 'chargeback'`) y clientes que se mudaron o cambiaron de segmento.

Tu pipeline debe integrar el lote 2 **sin duplicar nada**, actualizando lo que cambió, y debe poder re-ejecutarse sin alterar el resultado.

## A.0 · Generar los lotes

```bash
uv sync --extra dev
uv run python labs/shared/generate_data.py --start-date 2026-08-01 --days 60 \
    --transactions 30000 --dirty-rate 0.02 --output data/batch1
uv run python labs/shared/generate_data.py --start-date 2026-09-30 --days 1 \
    --transactions 2000 --dirty-rate 0.02 --from-batch data/batch1 --id-prefix B2 --output data/batch2
```

## A.1 · Desarrollar en local (gratis)

Desarrolla y prueba todo en tu computador antes de tocar AWS. Solo necesitas Docker: el script usa una imagen de Spark con Delta Lake, y los datos quedan en `lakehouse-local/`.

```bash
# Ingesta: valida tus TODO sin Spark
uv run pytest labs/02-lakehouse/tests/test_ingest.py

# Pipeline completo del lote 1, y luego solo un paso mientras desarrollas
bash labs/02-lakehouse/run_local.sh data/batch1 2026-09-29
bash labs/02-lakehouse/run_local.sh data/batch1 2026-09-29 silver

# Lote 2
bash labs/02-lakehouse/run_local.sh data/batch2 2026-09-30
```

> ✅ **Prueba de idempotencia:** ejecuta cada lote **dos veces**. La segunda vez todas las validaciones deben seguir en verde y la auditoría debe reportar 0 insertadas y 0 actualizadas. Algunos errores (por ejemplo, escribir Bronze con `append`) solo se notan en la segunda ejecución.

Para empezar de cero: `rm -rf lakehouse-local`.

---

# Parte B · Streaming

## El escenario: pagos en tiempo real

Además de los lotes nocturnos, PayTrack recibe cada pago **segundos** después de que ocurre. `producer.py` simula esa fuente con los problemas de un stream real:

| Lo que hace el productor | Por qué importa |
|--------------------------|-----------------|
| Pagos nuevos de los clientes del lote 1 | Deben llegar a `silver.transactions` junto a los del batch |
| **Contracargos** de pagos del lote 1 y del mismo stream | Actualizan el `status` de una transacción que ya existe |
| **Reenvíos** del mismo evento (mismo `event_id`) | La fuente entrega *al menos una vez*: hay que deduplicar |
| **Eventos tardíos** (su `event_time` es hasta 6 min anterior a la llegada) | El orden de llegada no es el orden en que ocurrieron |
| **Ráfagas**: 6 a 8 pagos del mismo cliente en segundos (`is_fraud = true`) | Lo que deben detectar las alertas |

Cada evento lleva `event_id`, `event_type` (`payment` o `chargeback`), `event_time` (cuándo ocurrió el evento) y los campos de la transacción.

El job `jobs/stream.py` corre **cuatro consultas de streaming**, cada una con su checkpoint:

1. **bronze**: del broker a `bronze/transaction_events`, tal como llegó (append).
2. **silver**: de bronze a `silver.transactions` con `foreachBatch` + `MERGE`. No usa watermark, así que ningún evento tardío se pierde.
3. **metrics**: ventanas fijas de 1 minuto por comercio, sobre los eventos deduplicados con watermark.
4. **alerts**: ventanas deslizantes de 30 s (cada 10 s) por cliente; 6 o más pagos generan una alerta.

El job corre en dos modos:

- `RUN_SECONDS = 0` (**availableNow**): procesa lo que hay en el broker y termina. Sirve para desarrollar.
- `RUN_SECONDS = N` (**en vivo**): un micro-lote cada 10 s durante N segundos. Al apagarse, termina de procesar lo que quedó en bronze.

> ⚠️ Requisito: `silver.transactions` debe existir. Corre primero la Parte A con el lote 1.

## B.1 · Productor (sin Kafka ni AWS)

Completa los TODO de `producer.py` hasta que los tests pasen:

```bash
uv run pytest labs/02-lakehouse/tests/test_producer.py -v
```

## B.2 · Streaming en local con Kafka

```bash
# Levantar Kafka (un broker, topics con 3 particiones)
docker compose -f .docker/docker-compose.yml up -d kafka

# Enviar 10 minutos de eventos de una vez (tiempo de evento simulado)
uv run python labs/02-lakehouse/producer.py --sink kafka --rate 20 --duration 600 --fast

# Procesar lo disponible y validar (modo availableNow)
bash labs/02-lakehouse/run_local.sh stream
```

Completa los TODO de `jobs/stream.py` hasta que todas las validaciones queden en verde.

> ✅ **Prueba de idempotencia:** vuelve a ejecutar `run_local.sh stream` sin enviar eventos nuevos. Todas las consultas deben leer 0 filas y las tablas no deben cambiar: los checkpoints recuerdan hasta dónde se procesó.

Después, pruébalo **en vivo** con dos terminales:

```bash
# Terminal 1: el job corre 3 minutos
bash labs/02-lakehouse/run_local.sh stream 180

# Terminal 2 (cuando la terminal 1 diga "Modo en vivo"): eventos en tiempo real
uv run python labs/02-lakehouse/producer.py --sink kafka --rate 20 --duration 120
```

Herramientas útiles:

```bash
# Ver algunos eventos del topic con su llave
docker compose -f .docker/docker-compose.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic paytrack.transactions --from-beginning --max-messages 5 \
  --property print.key=true

# Cuántos eventos tiene cada partición (reparto de la carga)
docker compose -f .docker/docker-compose.yml exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic paytrack.transactions
```

Para reiniciar solo el streaming: `docker compose -f .docker/docker-compose.yml rm -sf kafka` (borra el topic) y `rm -rf lakehouse-local`, y vuelve a correr el lote 1. Silver ya tiene los pagos del stream, así que no basta con borrar los checkpoints.

## B.3 · Experimentos

1. **Watermark.** Anota las filas *descartadas por watermark* de `metrics` en modo availableNow (`--fast`) y en vivo. ¿Por qué son tan distintas si el productor envía la misma proporción de eventos tardíos?
2. **Carga.** Repite la prueba en vivo con `--rate 50`. ¿Qué pasa con el número de alertas? Mide su precisión en AWS con el TODO 5 de Athena, a 20 y a 50 eventos/s.
3. **Partition key.** Cambia temporalmente `partition_key` para que devuelva un valor constante y envía eventos a un topic aparte (`--topic paytrack.experimento --duration 60 --fast`). Revisa el reparto con `kafka-get-offsets.sh --topic paytrack.experimento`. Repite con `transaction_id` en otro topic. Restaura tu llave al terminar.

---

# Ejecución en AWS (partes A y B)

## Desplegar

Completa el TODO de `infra/serverless.yml` y despliega:

```bash
set -a && source .envs/aws.env && set +a
cd labs/02-lakehouse/infra && serverless deploy --stage dev && cd -
```

El stack crea el bucket, los 5 jobs de Glue (4 batch y 1 de streaming), el stream de Kinesis (2 shards) y el workgroup de Athena.

> ⚠️ **Kinesis cobra por hora aunque no le llegue nada** (~USD 0.72 al día con 2 shards). Despliega cuando tengas todo listo en local, y haz la limpieza el mismo día.

## Ejecutar

```bash
# Parte A: los dos lotes
bash labs/02-lakehouse/run_aws.sh data/batch1 2026-09-29
bash labs/02-lakehouse/run_aws.sh data/batch2 2026-09-30

# Parte B: el job de Glue Streaming corre 5 minutos mientras el productor envía a Kinesis
bash labs/02-lakehouse/run_aws.sh stream 300
```

El job `paytrack-stream` es un job de **Glue Streaming** (Spark Structured Streaming) con el mismo `stream.py` que corriste en local. Solo cambia la fuente: lee Kinesis con el conector de Glue en lugar de Kafka (`read_events` en `jobs/common.py`). Lo que imprime cada job, incluidas las validaciones, queda en CloudWatch: `/aws-glue/jobs/output` y `/aws-glue/jobs/error`.

## Consultar con Athena

Abre la consola de Athena, elige el workgroup `paytrack-lakehouse-wg` y ejecuta `sql/athena.sql`: primero el DDL dado y luego tus 6 consultas. Anota los MB escaneados.

## Limpieza (obligatoria)

```bash
BUCKET=$(aws cloudformation describe-stacks --stack-name paytrack-lakehouse-dev \
  --query "Stacks[0].Outputs[?OutputKey=='LakehouseBucket'].OutputValue" --output text)
aws s3 rm s3://$BUCKET --recursive
cd labs/02-lakehouse/infra && serverless remove --stage dev
```

`serverless remove` elimina también el stream de Kinesis. Verifica en la consola de Kinesis que no quede ningún stream.

---

## Preguntas para el informe

**Parte A · Batch**

1. **Bronze.** ¿Por qué Bronze lee todo como texto? ¿Qué problema evita `replaceWhere` frente a `append`? Muéstralo con lo que pasó en tu prueba de idempotencia.
2. **Silver.** ¿Cuántas filas quedaron en cuarentena por cada motivo en cada lote? ¿Por qué se deduplica **antes** de aplicar las reglas de calidad?
3. **MERGE.** ¿Por qué la actualización solo debe ocurrir cuando el `status` cambió? ¿Qué reportaría la auditoría si quitaras esa condición y re-ejecutaras un lote?
4. **Time travel.** ¿Cuántos contracargos encontró la auditoría y qué monto revierten? ¿Qué pasaría con esta auditoría después de un `VACUUM`?
5. **PII.** `silver.customers` se actualiza sobrescribiendo los cambios (tipo 1). ¿Qué análisis de negocio se pierde? ¿Cómo lo resolverías con lo que hiciste en el Lab 1?
6. **Consumo.** Compara los MB escaneados del TODO 1 y del TODO 2 de Athena. ¿Por qué existe Gold si Silver ya responde la pregunta? ¿Qué pierdes?

**Parte B · Streaming**

7. **Garantías de entrega.** ¿Qué garantía da tu productor: *at-most-once*, *at-least-once* o *exactly-once*? ¿Y Kinesis con tus reintentos? Explica qué combinación de checkpoint, Delta y `MERGE` hace que las tablas queden como si cada evento se hubiera procesado exactamente una vez. Usa tu prueba de idempotencia como evidencia.
8. **Watermark.** Responde el experimento 1. ¿Por qué la consulta `silver` no usa watermark? ¿Qué pasaría con los pagos tardíos si lo usara?
9. **Latencia.** ¿Cuánto tarda en aparecer una alerta en `gold.fraud_alerts` después del último pago de la ráfaga? ¿Qué parte de ese tiempo se debe al modo *append* y al watermark? ¿Cómo la bajarías y qué perderías?
10. **Precisión.** Reporta la precisión de las alertas (TODO 5) a 20 y a 50 eventos/s. ¿Por qué cambia? ¿Qué ajustarías para mejorarla?
11. **Estado final.** En el TODO 4, ¿qué escenario concreto del productor revertiría un contracargo si quitaras la condición sobre `'chargeback'`? ¿Por qué `latest_per_transaction` no basta para evitarlo?
12. **Partition key.** Con el experimento 3: ¿cómo se repartió la carga con cada llave? ¿Cambiaría el número de alertas que detecta Spark con `transaction_id` como llave? Si Spark agrupa por cliente de todas formas, ¿para qué sirve la partition key? (piensa en el contracargo y su pago).
13. **Batch y streaming en la misma tabla.** Revisa `DESCRIBE HISTORY` de `silver.transactions` después del streaming: ¿cuántas versiones creó?, ¿cuántas filas reescribió cada `MERGE` (`numTargetRowsCopied`)? ¿Qué problema anticipas a 500K pagos/día y cómo lo resolverías? ¿Qué pasaría si el job silver batch y el stream escribieran al mismo tiempo? Con el TODO 6 de Athena: ¿cuál es la fuente de verdad, la capa rápida o la batch?
14. **Costos y operación.** Estima el costo mensual de las dos partes con 500K transacciones diarias (S3, Glue, Glue Streaming 24/7, Kinesis y Athena). ¿Qué cambiarías si el streaming no necesitara correr 24/7? ¿Por qué no se debe poner una regla de expiración de S3 sobre `lakehouse/`?

## Entregables

- [ ] Tu código: `ingest.py`, `producer.py`, los 5 jobs, `athena.sql` y `serverless.yml`
- [ ] Captura de `pytest` en verde (los dos archivos de tests)
- [ ] Salida de `run_local.sh` para los dos lotes, incluida la segunda ejecución del lote 2 (idempotencia)
- [ ] Salida de `run_local.sh stream` en modo availableNow (dos veces: idempotencia) y en vivo
- [ ] Captura de los 4 jobs batch en `Succeeded` para los dos lotes, y del job `paytrack-stream` en `Succeeded`
- [ ] Resultados y MB escaneados de las 6 consultas de Athena
- [ ] Diagrama de la arquitectura hecho en [draw.io](https://app.diagrams.net/): zonas, tablas por capa, jobs, consultas de streaming y servicios de AWS
- [ ] Informe en PDF (máx. 8 páginas) con las respuestas y los experimentos
- [ ] **Recursos eliminados**, incluido el stream de Kinesis

## Criterios de evaluación

| Criterio | Peso |
|----------|------|
| Batch: ingesta y Bronze correctos e idempotentes | 10% |
| Batch: Silver con calidad, cuarentena, MERGE y PII (validaciones en verde) | 15% |
| Batch: Gold y auditoría con time travel (validaciones en verde) | 10% |
| Streaming: productor (tests) y job stream (validaciones en verde, idempotente) | 20% |
| Pipeline ejecutado en AWS (batch y streaming) y consultas de Athena | 15% |
| Respuestas, experimentos y análisis del informe | 20% |
| Diagrama y calidad del informe | 10% |

## ⚠️ Costos estimados

Si desarrollas en local y solo usas AWS para la ejecución final, el mismo día: **~USD 2**.

| Servicio | Costo estimado |
|----------|---------------|
| S3 (< 1 GB) | Gratis (free tier) |
| Glue 5.0 batch (2 workers, ~1–2 min por job; 4 jobs × 2 lotes) | ~USD 0.03 por job |
| Glue Streaming (2 workers G.1X, ~6 min por ejecución) | ~USD 0.10 por ejecución |
| Kinesis Data Streams (2 shards) | ~USD 0.03 por hora **mientras exista el stream** |
| Athena (< 100 MB escaneados) | < USD 0.01 |

Cada ejecución de un job de Glue cobra mínimo 1 minuto. Depurar directamente en Glue es lento y cuesta: para eso está `run_local.sh`. El riesgo real es olvidar el stream de Kinesis: cuesta ~USD 22 al mes aunque no se use.
