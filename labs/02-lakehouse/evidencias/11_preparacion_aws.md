# Lab 2 · Preparación para la sesión conjunta en AWS

> Nota de preparación. **Nada de esto se ha ejecutado en AWS.** Las consultas de
> Athena y el despliegue se validan en la sesión conjunta. No hay resultados,
> tiempos, costos ni MB escaneados medidos; esos campos quedan por llenar allí.

## 1. Ciclo de vida de la zona `raw/` (serverless.yml)

**Decisión:** una regla de lifecycle **exclusiva del prefijo `raw/`** con
**transiciones** y **sin expiración**:

- `raw/` → **STANDARD_IA** a los **30 días**.
- `raw/` → **GLACIER_IR** a los **90 días**.
- **Sin `ExpirationInDays`** en `raw/`.

**Justificación:**
- `raw/` es la **fuente de verdad** para **auditoría** y **reprocesamiento**: un lote
  debe poder re-ingerirse tal como llegó, así que **no se borra nunca**.
- El reprocesamiento habitual ocurre en **días**, no en meses; pasado ese plazo el
  acceso es infrecuente, de modo que **STANDARD_IA** (30 d) y luego **GLACIER_IR**
  (90 d, recuperación inmediata) **abaratan** el almacenamiento sin perder el dato ni
  la disponibilidad para una auditoría puntual.
- La regla se aplica **solo** a `raw/`. **No** toca `lakehouse/`, `checkpoints/` ni
  `athena-results/` (esta última conserva su propia regla de expiración a 7 días, ya
  dada).
- **Por qué NO expirar `lakehouse/`** (para el informe): las tablas Delta
  reescriben/compactan archivos y el `_delta_log` referencia versiones previas;
  expirar objetos por antigüedad rompería el **time travel** y podría dejar la tabla
  inconsistente. En su lugar se usa la gestión propia de Delta (**OPTIMIZE** y
  **VACUUM** con retención controlada), no reglas de S3 por edad.

No se modificó ningún otro recurso ni parámetro del stack.

## 2. Qué responde cada consulta nueva de `athena.sql`

### TODO 5 — Precisión de las alertas de ráfagas
- **Qué responde:** de las alertas en `gold_fraud_alerts`, qué porcentaje corresponde
  a clientes que **de verdad** cometieron fraude dentro de la ventana de la alerta.
- **Criterio de acierto:** una alerta (`window_start`, `window_end`, `customer_id`) es
  un **acierto** si **algún pago** de ese cliente en `bronze_transaction_events`, con
  `event_time ∈ [window_start, window_end)`, tiene `is_fraud = true`.
- **Cómo está calculada:** `LEFT JOIN` de la alerta con los pagos del mismo cliente en
  la ventana, colapsado con `MAX(CASE ... THEN 1 ELSE 0 END)` **agrupando por la llave
  de la alerta**. Así cada alerta cuenta **una sola vez** y **no se multiplica** por
  los pagos del JOIN. `precision_pct = 100 * aciertos / NULLIF(total_alertas, 0)`:
  si no hay alertas, da **NULL** (no divide por cero).
- **Cómo interpretar:** `total_alertas`, `aciertos` y `precision_pct`. Una precisión
  baja significa que las ventanas de 6+ pagos incluyen clientes legítimos con mucha
  actividad (falsos positivos); alta, que casi todas las ráfagas detectadas eran
  fraude real.

### TODO 6 — Capa rápida vs. capa batch
- **Qué responde:** por **día y comercio**, cuántos pagos ve la **capa rápida**
  (`gold_merchant_minute_metrics`, suma de `num_payments` de las ventanas) frente a
  cuántas **transacciones del stream** hay en la **capa batch**
  (`silver_transactions` con `transaction_id LIKE 'TXNS%'`), y la **diferencia**.
- **Grano y alcance:** la capa rápida agrega por `date(window_start)` (solo ventanas ya
  cerradas por el watermark); la capa batch cuenta por `date(event_ts)`. `FULL OUTER
  JOIN` para no perder filas de una capa ausente en la otra.
- **Cómo interpretar la diferencia:** suele ser **positiva a favor de silver** porque
  (a) hay **ventanas que aún no cerró el watermark** y por tanto no están en métricas;
  y (b) las dos capas miden cosas distintas (métricas = **pagos** `event_type='payment'`
  deduplicados; silver = **transacciones** por `transaction_id`, incluye contracargos y
  colapsa varios eventos de una misma transacción). No es un error: es la naturaleza
  *rápida pero eventual* de la capa de streaming frente a la *completa* de silver.

### Nota de alcance
- No se tocó el **DDL** dado ni los **TODO 1–4** de Samuel.
- Los nombres de tablas/columnas usados coinciden con los esquemas reales:
  `gold_fraud_alerts(window_start, window_end, customer_id, num_payments, total_cop)`,
  `gold_merchant_minute_metrics(window_start, window_end, merchant_id,
  merchant_category, num_payments, approved_cop, approval_rate_pct)`,
  `bronze_transaction_events(... event_type, event_time, customer_id, is_fraud ...)`,
  `silver_transactions(... transaction_id, merchant_id, event_ts, status ...)`.

## 3. Medir precisión a 20 vs 50 eventos/s sin mezclar alertas acumuladas

El TODO 5 calcula precisión **sobre toda `gold_fraud_alerts`**, que es **acumulativa**
(append de todas las corridas locales). Para separar la medición de la corrida a 20 de
la de 50 eventos/s en AWS, **sin mezclar alertas de distintas corridas**, usar los
campos de ventana ya existentes para acotar por tiempo:

- Las alertas llevan `window_start`/`window_end` (timestamps del `event_time` del
  evento). Cada corrida del productor usa una **ventana temporal distinta** de
  `event_time`; filtrar `gold_fraud_alerts` por el rango `[window_start, window_end)`
  que corresponde a cada envío **aísla** sus alertas.
- Procedimiento sugerido en AWS (a confirmar en la sesión): correr **primero** el
  envío a 20 ev/s, anotar su rango de `event_time`; luego el de 50 ev/s con un rango
  separado; y aplicar el TODO 5 **con un `WHERE` sobre `window_start`** por cada rango.
  Alternativa más limpia: desplegar/medir cada rate en una **ejecución con tablas
  frescas** para no acumular.
- **No** basta con contar "más alertas" en una corrida: más alertas **no** implican
  mejor precisión (lo discute `08_resumen_experimentos.md`). La métrica a comparar es
  el **`precision_pct`** de cada rango, no el número de alertas.

## 4. Qué falta ejecutar y medir en AWS (pendiente)

- **Desplegar** el stack (`serverless deploy --stage dev`) y subir los scripts; verificar
  la regla de lifecycle de `raw/` en la consola de S3.
- **Correr** los 4 jobs batch por los dos lotes y el job `paytrack-stream`; confirmar
  `Succeeded` y revisar validaciones en CloudWatch.
- **Ejecutar** en Athena el DDL y las 6 consultas; anotar **resultados** y **MB
  escaneados** (TODO 1 vs 2 incluido).
- **Medir precisión** de alertas (TODO 5) a **20** y **50** ev/s por separado, con el
  filtrado por ventana descrito arriba.
- **Limpieza obligatoria**: vaciar el bucket, `serverless remove`, y confirmar que **no
  queda el stream de Kinesis** (cobra por hora).
