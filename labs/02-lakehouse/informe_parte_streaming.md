# Lab 2 · Parte B (Streaming) — Respuestas 7–14

> Borrador para integrar al informe conjunto (máx. 8 páginas). Cubre la parte de
> streaming: `producer.py`, `jobs/stream.py` y los TODO 5–6 de `sql/athena.sql`.
> Las cifras provienen de `labs/02-lakehouse/evidencias/`. Lo que depende de AWS se
> marca **[pendiente AWS]**. Diagrama: `PayTrack_lakehouse.drawio`.

---

## 7. Garantías de entrega (at-most / at-least / exactly-once)

**Implementado.** El productor Kafka usa `acks="all"` y `enable.idempotence=True`
(`producer.py`, `kafka_config`): el broker confirma con réplicas y el cliente no
duplica por reintentos *dentro de una sesión del productor*. Para Kinesis,
`put_with_retries` reenvía **solo** los registros con `ErrorCode` (reintento parcial
con backoff exponencial), devolviendo cuántos quedaron sin enviar.

Hay que distinguir cuatro mecanismos, porque ninguno da exactly-once por sí solo:

1. **Idempotencia del productor Kafka**: evita duplicados por reintentos del cliente,
   pero **no** cubre reenvíos *de la aplicación*.
2. **Reenvíos de la aplicación**: el productor **reenvía a propósito** eventos con el
   mismo `event_id` (simula una fuente *at-least-once*). Esto es un duplicado real a
   nivel de dato, no de transporte.
3. **Checkpoints de Structured Streaming**: cada consulta (bronze, silver, metrics,
   alerts) guarda su progreso; tras un reinicio no reprocesa ni pierde offsets.
4. **MERGE + Delta en silver**: `deduplicate_events` (watermark + `event_id`+`event_time`),
   `latest_per_transaction` y el `MERGE` por `transaction_id` hacen que los reenvíos y
   los eventos tardíos converjan al mismo estado final.

La combinación **checkpoint + Delta transaccional + MERGE idempotente** da un efecto
**equivalente a exactly-once en la tabla** (*effectively-once*): aunque un evento se
entregue o reprocese varias veces, `silver.transactions` queda igual. **No se promete
exactly-once absoluto de extremo a extremo** (la fuente es at-least-once y Kinesis con
reintentos es at-least-once).

**Evidencia:** idempotencia comprobada en `evidencias/04b`/`04c` (2ª corrida: tablas
idénticas) y en `evidencias/10f`/`10g` (las 4 consultas leen 0 filas, conteos
estables). Kinesis es de diseño (no ejecutado): **[pendiente AWS]**.

---

## 8. Watermark

**Implementado.** `metrics` y `alerts` usan watermark de 2 min sobre `event_time`
(`WATERMARK="2 minutes"`); `silver` **no** usa watermark (MERGE por micro-lote) para no
perder pagos tardíos; `bronze` es append puro.

**Observado (contadores corregidos, `evidencias/08` §2 y §3):** descartes por watermark
por consulta:

| Corrida | metrics | alerts |
|---|---|---|
| availableNow (`--fast`, `03*`) | 4 | 4 |
| en vivo rate 20 (`05*`, ventana + drenaje) | 4 + 1 = 5 | 4 + 1 = 5 |
| en vivo rate 50 (`06*`, ventana + drenaje) | 90 + 12 = 102 | 90 + 12 = 102 |

`bronze` y `silver`: 0 descartes en todas las corridas.

**Por qué silver no usa watermark:** si lo usara, un pago **tardío** (su `event_time`
puede ir hasta 6 min atrás) cuyo retraso superara el watermark se **descartaría** y esa
transacción nunca llegaría a silver. Como silver es la fuente de verdad, debe aceptar
todos los pagos, aun desordenados; por eso se integra con MERGE sin watermark.

**Limitaciones (documentadas en `08`):** las corridas comparten topic/checkpoints/tablas
(estado acumulado), tienen duraciones y volúmenes distintos y el modo `--fast` usa reloj
simulado (duración real no medida). Por eso **no se atribuye** el aumento de descartes
únicamente al rate. El experimento formal del README (watermark en `--fast` vs en vivo)
queda como comparación **indicativa**, no controlada.

---

## 9. Latencia de las alertas

**Implementado.** `alerts` usa ventana deslizante de 30 s que avanza cada 10 s, en modo
**append**: una ventana solo se emite cuando el **watermark** (2 min) la cierra. Por
diseño, la latencia tiene tres componentes: (a) el avance del watermark hasta cubrir el
fin de la ventana, (b) el tamaño/paso de la ventana y (c) el intervalo de micro-lote
(10 s en vivo).

**No se midió la latencia observada.** Las evidencias registran conteos y descartes,
no marcas de tiempo de "último pago de la ráfaga" vs "aparición de la alerta". Reducirla
(menor watermark o `update`/`complete` en vez de `append`) **cambiaría el resultado**:
con menos watermark se perderían más tardíos; con `update` se emitirían filas parciales
revisables. **Medición de latencia: [pendiente]** (requiere instrumentar tiempos; no se
hace en esta etapa).

---

## 10. Precisión de las alertas

**Implementado.** `fraud_alerts` cuenta pagos por cliente en la ventana y marca alerta
con ≥6; **no filtra por `is_fraud`** a propósito, para poder medir precisión aparte. El
TODO 5 de `athena.sql` calcula la precisión: una alerta es acierto si **algún** pago del
cliente dentro de `[window_start, window_end)` tiene `is_fraud=true` en
`bronze_transaction_events`, contando cada alerta **una vez** (ver `evidencias/11` §2).

**Más alertas ≠ mejor precisión.** En local se observó que a rate 50 hubo más alertas
nuevas (+27) que a rate 20 (+18) (`evidencias/08` §3), pero eso refleja volumen/ráfagas,
no calidad. **Las cifras de precisión a 20 y 50 ev/s son [pendiente AWS]**: se obtienen
con el TODO 5 en Athena, separando cada rate por el rango de `window_start`
(procedimiento en `evidencias/11` §3). No se reporta ningún `precision_pct` todavía.

---

## 11. Estado final: por qué `chargeback` es terminal

**Implementado.** El MERGE de silver (en `merge_into_silver`) actualiza solo cuando
`t.status <> s.status` **y** `t.status <> 'chargeback'`: un contracargo ya aplicado es
**estado final** y no se revierte.

**Escenario que lo revertiría sin esa condición:** un contracargo y el pago original
comparten `transaction_id`. Si un **reenvío** del pago original (mismo `transaction_id`,
`status='approved'`) o un **evento tardío** de ese pago llega **después** del contracargo,
sin la guarda `t.status <> 'chargeback'` el MERGE volvería el estado a `approved`,
**deshaciendo** el contracargo.

**Por qué `latest_per_transaction` no basta:** esa función elige el evento más reciente
**dentro de un micro-lote**, por `event_time`. Pero el pago original y su contracargo
pueden llegar en **micro-lotes distintos**, y un evento tardío del pago tiene un
`event_time` **anterior** al contracargo pero **llega después**. Elegir "el más reciente
del micro-lote" no protege contra un pago tardío que aterriza en un lote posterior; la
condición terminal en el MERGE sí. **Evidencia:** validación "ningún contracargo fue
revertido por un reenvío" en ✓ (`evidencias/03b`, `05a`, `06b`, `10c`).

---

## 12. Partition key (reparto y orden)

**Implementado.** `partition_key = str(customer_id)`. Experimento con topics
dedicados de 3 particiones, `--rate 20 --duration 60 --fast` (`evidencias/07d`):

| Llave | Enviados | P0 / P1 / P2 | % P0 / P1 / P2 |
|---|---|---|---|
| `customer_id` | 1 207 | 460 / 339 / 408 | 38,1 / 28,1 / 33,8 |
| constante | 1 230 | 1 230 / 0 / 0 | 100 / 0 / 0 |
| `transaction_id` | 1 224 | 391 / 429 / 404 | 31,9 / 35,0 / 33,0 |

**Lectura:** `customer_id` reparte entre las 3 particiones (no uniforme) y mantiene
juntos todos los eventos de un cliente; la **constante** colapsa todo a una partición
(anti-patrón, sin paralelismo); `transaction_id` reparte más parejo.

**Orden por partición ≠ orden por `event_time`.** La partition key fija **a qué
partición** va cada evento y preserva el **orden de llegada (offset)** dentro de esa
partición; **no** garantiza orden cronológico por `event_time` (los tardíos llegan con
`event_time` anterior pero se colocan en el offset de su llegada). El orden temporal lo
recupera el watermark/las ventanas, no la llave.

**Número de alertas con `transaction_id`:** Spark agrupa las alertas por `customer_id`
sobre `event_time`, independientemente de la partición física, así que el número de
alertas **no debería** cambiar por la partition key (análisis de diseño; no se
re-ejecutó el pipeline sobre esos topics de experimento). La llave sirve para
**reparto/paralelismo** y **orden por partición**; **no** garantiza orden global ni
resuelve los reintentos parciales de Kinesis (eso lo hace `put_with_retries`).

---

## 13. Batch y streaming en la misma `silver.transactions`

**Implementado.** Batch (`jobs/silver.py`) y streaming (`jobs/stream.py`,
`foreachBatch`+MERGE) escriben en la **misma** tabla Delta `silver/transactions`.

**`DESCRIBE HISTORY` local (solo lectura, `evidencias/12_silver_history_local.txt`) —
resultados LOCALES, no AWS:**

- **21 versiones**: v0 `WRITE` (vacía) + **20 `MERGE`**.
- Cada MERGE reescribe archivos: `numTargetRowsCopied` crece mucho frente a las filas
  que realmente cambian. Ejemplos:
  - v2 (lote 2 batch): insertó 3 802, actualizó 84, **copió 28 165**.
  - v20 (último MERGE del stream): insertó 1 828, actualizó 27, **copió 32 299**.
- Totales sobre los 20 MERGE: insertadas 48 169, actualizadas 401, **copiadas 537 830**.

**Qué anticipa a 500K pagos/día:** el MERGE por micro-lote reescribe archivos enteros
que contienen muchas filas sin cambios (`numTargetRowsCopied` ≫ filas modificadas): es
**amplificación de escritura**. A gran volumen se mitiga con **particionado/ordenamiento**
que localice el MERGE (ej. por fecha), **OPTIMIZE/Z-ORDER**, micro-lotes más grandes y
menos frecuentes, y limpieza con **VACUUM**.

**Concurrencia batch+stream:** si el job silver batch y el stream hicieran MERGE **a la
vez** sobre la misma tabla, Delta usa control de concurrencia optimista; un
`ConcurrentModificationException` puede abortar una de las transacciones (debe
reintentar). La práctica segura es **no** solaparlos en el tiempo (o particionar para
que no toquen los mismos archivos).

**Capa rápida vs. silver (TODO 6):** `gold_merchant_minute_metrics` cuenta **pagos**
(`event_type='payment'`, deduplicados, solo ventanas **cerradas** por watermark);
`silver` cuenta **transacciones** por `transaction_id` (incluye contracargos, colapsa
varios eventos por transacción). La diferencia esperada viene de ventanas aún abiertas y
de medir cosas distintas. **La consulta Athena 6 NO se ha ejecutado** (su resultado y MB
son **[pendiente AWS]**); aquí solo se explica el porqué. "Fuente de verdad" = la capa
batch (silver); la rápida es un adelanto eventual.

---

## 14. Costos y operación

**Implementado (lo que impacta costo):** lifecycle de S3 **solo** en `raw/` →
STANDARD_IA (30 d) y GLACIER_IR (90 d), **sin expiración** (auditoría/reprocesamiento).
`lakehouse/` no lleva expiración por edad (rompería el time travel de Delta); se gestiona
con OPTIMIZE/VACUUM. Streaming en Glue con `RUN_SECONDS` acotado y Kinesis de 2 shards.

**No se inventan precios ni MB.** Tabla de supuestos y componentes **pendientes de
cotizar** con los números reales de la corrida en AWS:

| Componente | Qué cotizar | Supuesto / variable | Dato pendiente |
|---|---|---|---|
| S3 | almacenamiento por clase + nº de objetos | 500K txn/día; tiering raw/ 30/90 d | GB en STANDARD/IA/GLACIER_IR |
| Glue batch | DPU-hora × 4 jobs × 2 lotes | 2× G.1X, mín. 1 min/job | duración real por job |
| Glue Streaming | DPU-hora × tiempo encendido | 2× G.1X; **24/7 vs por ventana** | horas/día que corre |
| Kinesis | shard-hora (+ PUT) | 2 shards; **cobra aunque no llegue nada** | horas de vida del stream |
| Athena | USD por TB escaneado | 6 consultas | **MB escaneados** (TODO 1–6) |

**Operación:** si el streaming **no** necesita 24/7, se enciende por ventana (como en
local con `RUN_SECONDS`) y se baja el costo dominante (Glue Streaming + Kinesis). El
riesgo real es **dejar Kinesis vivo**: cobra por hora aunque no reciba datos.
**Estimación mensual: [pendiente AWS]** (necesita las duraciones y MB de la corrida
real).

---

## Lista para la sesión conjunta con Samuel

- **Athena 1–4**: pendientes de Samuel (parte batch).
- **Ejecuciones en AWS**: desplegar stack; correr los 4 jobs batch (lotes 1 y 2) y
  `paytrack-stream`; confirmar `Succeeded` y validaciones en CloudWatch.
- **Precisión a 20 y 50 ev/s** (TODO 5): medir por separado filtrando por `window_start`
  (ver `evidencias/11` §3). Hoy **sin cifra**.
- **Latencia de alertas**: sigue **sin medición**; decidir si se instrumenta en AWS.
- **6 consultas Athena**: registrar **resultados** y **MB escaneados** (incl. TODO 6 y
  la comparación TODO 1 vs 2).
- **Datos para costos**: duraciones reales de jobs, horas de Glue Streaming/Kinesis, GB
  en S3 por clase, MB de Athena.
- **Capturas y limpieza**: pytest en verde (ya: `evidencias/09`), `run_local.sh` de los
  dos lotes y del stream (ya: `evidencias/01`, `03b`, `05a`, `06b`, `10c`); en AWS,
  capturas de jobs `Succeeded` y de Athena; **vaciar bucket + `serverless remove` +
  confirmar que no queda el stream de Kinesis**.
