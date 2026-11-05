# Lab 2 · Parte 3 — Experimentos locales y análisis

> Todas las cifras provienen de las evidencias en esta carpeta. Se distingue lo
> **comprobable** (en los logs) de las **hipótesis**. No se midió precisión de
> alertas: eso requiere el TODO 5 de Athena en AWS y queda pendiente.

## 0. Cómo leer los contadores de `report()` y de las tablas

- `report()` en `stream.py` suma sobre `q.recentProgress`, que **solo retiene los
  últimos progresos** (config `spark.sql.streaming.numRecentProgressUpdates`, por
  defecto 100). No es, en general, el total de toda la vida de la consulta.
  - En **nuestras** ejecuciones cada consulta tuvo muchos menos de 100 micro-lotes
    (availableNow ≤ 7; ventana en vivo ≤ 25; drenaje ≤ 2), así que la suma **sí
    cubrió todos los micro-lotes de esa ejecución**: para estos casos equivale al
    total de la ejecución, sin doble conteo (cada progreso aparece una vez).
  - En modo en vivo `report()` se llama **dos veces** sobre **instancias de consulta
    distintas** (la ventana en vivo y el drenaje availableNow son dos
    `start_downstream` separados). Sus arreglos de progreso son disjuntos, así que
    **sumarlos es correcto** (no es doble conteo).
- Las tablas Delta (`silver`, `metrics`, `alerts`) son **acumulativas** (append /
  merge entre ejecuciones). Por eso se reportan **conteos antes/después** y el
  **delta por ejecución**, no solo el total acumulado.

## 1. IDs entre ejecuciones y el salto de +621 en Silver (en vivo, rate 20)

**Comprobable:**
- `transaction_id = "TXNS" + uuid.UUID(int=rng.getrandbits(128)).hex[:12]` y
  `event_id = uuid.UUID(int=rng.getrandbits(128))`, ambos de `random.Random(seed)`.
- Todas nuestras ejecuciones corrieron **sin `--seed`** → `random.Random(None)`
  siembra de la entropía del SO → **cada ejecución genera IDs nuevos**. Colisión de
  `transaction_id` entre ejecuciones: prácticamente nula (12 hex ~ 48 bits, con
  pocos miles de IDs el riesgo es despreciable; `event_id` es un UUID completo).
- Los **reenvíos** reusan un evento ya enviado (mismo `event_id` **y** `event_time`),
  por eso `deduplicate_events` los elimina y `latest_per_transaction` deja una fila
  por `transaction_id`.
- **Qué estados acepta Silver (comprobable en el código, TODO 4):** el `MERGE` hace
  `whenNotMatchedInsertAll` → **inserta toda transacción con `transaction_id` nuevo,
  sea cual sea su `status`** (incluidos `declined`). Para una transacción que ya
  existe, solo **actualiza** si el status cambió y el status actual no es
  `chargeback`; no inserta una fila nueva. El contracargo **comparte** el
  `transaction_id` de su pago, así que normalmente **actualiza**, no inserta.
- En la ejecución en vivo a rate 20: el productor envió **2 443** eventos; la consulta
  `bronze` **persistió 659** (12 975 − 12 316, comprobable con `06a`/`04c` y con el
  `numInputRows` de bronze en `05a`). Silver creció **+621** (39 968 → 40 589).

**Reconciliación correcta (comprobable, log `05a`):**
- `659` es lo que **bronze persistió desde Kafka** en esa corrida. En el mismo log,
  `silver` leyó **440** filas (ventana) **+ 219** (drenaje) **de la tabla bronze**:
  es **otra etapa**, no eventos nuevos de Kafka. **No se deben sumar**
  `bronze-desde-Kafka` + `silver-desde-bronze` (ese fue el error del resumen previo).
- De los **659** eventos persistidos, el neto de **inserciones** en Silver fue **621**.
  La diferencia es **659 − 621 = 38** eventos que **no** generaron fila nueva.

**Lo no determinado (sin atribución causal):**
- Esos 38 pueden deberse a reenvíos deduplicados, a contracargos que actualizan en vez
  de insertar, o a varios eventos con el mismo `transaction_id` en un micro-lote que
  colapsan en una fila. **No se puede decir cuántos de cada tipo** con estas
  evidencias: no se registró `numTargetRowsInserted/numTargetRowsUpdated` por
  micro-lote del `MERGE`, ni un conteo por `event_type`/`status` de esos 659.
  **El desglose del 38 queda como no determinado.** No se afirma que sea
  imprescindible una prueba nueva para resolverlo; simplemente, con lo registrado no
  se puede descomponer.

## 2. Experimento de watermark: availableNow (--fast) vs en vivo (rate 20)

| Métrica | availableNow (`--fast`) | En vivo (rate 20) |
|---|---|---|
| Evidencia | `03a`, `03b` | `05a`, `05b` |
| Modo de envío | acelerado (`--fast`, reloj simulado); **duración real no medida** (el log imprime "en 0s") | 120 s (tiempo real) |
| Eventos enviados | 12 316 | 2 443 |
| Eventos **persistidos por bronze desde Kafka** (esta corrida) | 12 316 | 659 |
| Descartes watermark · metrics | **4** | **4** (ventana) **+1** (drenaje) = **5** |
| Descartes watermark · alerts | **4** | **4** (ventana) **+1** (drenaje) = **5** |
| Descartes watermark · bronze / silver | 0 / 0 | 0 / 0 |

> Nota: el `numInputRows` de `silver`/`metrics`/`alerts` mide filas leídas **de la
> tabla bronze**, no eventos nuevos de Kafka. Solo `bronze` lee de Kafka. En la tabla
> de arriba, "persistido por bronze" es el delta de la tabla bronze de esa corrida.

**Diferencia tiempo de evento vs llegada vs watermark (comprobable por diseño):**
- `event_time` = cuándo ocurrió el evento (lo pone el productor; los **eventos tardíos**
  lo retrasan 30–360 s).
- Llegada (`arrival_ts`) = cuándo entró al broker.
- El **watermark** avanza según el **máximo `event_time` visto − 2 min**
  (`WATERMARK = "2 minutes"`). Una fila se descarta en `metrics`/`alerts` si su
  `event_time` cae por detrás del watermark cuando llega.
- `silver` **no usa watermark** (merge por micro-lote) → 0 descartes: ningún pago
  tardío se pierde. `bronze` es append puro → 0 descartes.

**Lo observado, sin presuponer:** en nuestros datos los descartes fueron **muy
parecidos en términos absolutos** (4 vs 5 por consulta), pese a enviarse 5× más
eventos en availableNow. No se observa aquí la gran diferencia que a veces se asume.
Posible razón (**hipótesis**): en `--fast` el reloj es simulado y el máximo `event_time`
salta rápido, cerrando ventanas de golpe; en vivo el watermark avanza al ritmo real.
Pero con estos volúmenes el número de tardíos que caen fuera es pequeño en ambos casos.

**Limitaciones de la comparación:**
- Modos y volúmenes distintos: availableNow con envío acelerado (12 316 eventos,
  duración real no medida) vs en vivo (2 443 eventos en 120 s). No son comparables
  como tasas.
- **Estado acumulado**: la corrida en vivo partió de checkpoints y tablas ya pobladas
  por availableNow; no son entornos limpios equivalentes.
- Los descartes de `report()` son por ejecución (ver §0), no comparables como totales
  de vida de la consulta.
- Que los descartes sean parecidos (4 vs 5) es **lo observado**; no se afirma que
  deban diferir ni se atribuye la diferencia a una sola causa.

## 3. Experimento de carga: rate 20 vs rate 50 (en vivo, duration 120)

> ⚠️ **Comparación afectada por estado acumulado y trabajo pendiente.** Las dos
> corridas no parten de cero: comparten topic, checkpoints y tablas. Tómese como
> indicativa, no como una medición controlada de "rate 20 vs rate 50".

| Métrica | rate 20 (`05*`) | rate 50 (`06*`) |
|---|---|---|
| Eventos enviados por el productor | 2 443 | 6 207 |
| Perdidos en el productor | 0 | 0 |
| Eventos **persistidos por bronze desde Kafka** (Δ tabla bronze) | 659 | 6 074 |
| `numInputRows` silver desde tabla bronze (ventana + drenaje) | — | 5 563 + 511 |
| Silver antes → después (Δ) | 39 968 → 40 589 (**+621**) | 40 589 → 46 341 (**+5 752**) |
| Ventanas metrics antes → después (Δ) | 98 → 126 (**+28**) | 126 → 170 (**+44**) |
| Alertas antes → después (Δ) | 156 → 174 (**+18**) | 174 → 201 (**+27**) |
| Descartes watermark · metrics (ventana + drenaje) | 4 + 1 = **5** | 90 + 12 = **102** |
| Descartes watermark · alerts (ventana + drenaje) | 4 + 1 = **5** | 90 + 12 = **102** |
| Validaciones | todas ✓ | todas ✓ |

**Observado (no causal):**
- Los descartes por watermark fueron mucho mayores en la corrida rate 50 (5 → 102 por
  consulta) y las alertas nuevas subieron (+18 → +27). Esto es **lo observado**.
- **No se atribuye únicamente al rate.** Las dos corridas difieren además en: estado
  acumulado (checkpoints/tablas previas), eventos pendientes en el topic al iniciar,
  número de micro-lotes y distinto solapamiento productor↔ventana en vivo. Con un solo
  par de corridas no se puede aislar el efecto del rate del de estas otras variables.

**Importante (no confundir):** más alertas **no** implica mejor **precisión**. El
número de alertas depende del volumen y de las ráfagas generadas; la precisión
(cuántas alertas corresponden a `is_fraud` real) **no se mide aquí**. `fraud_alerts`
**no** filtra por `is_fraud` a propósito, justamente para poder evaluar precisión
aparte. La medición formal es el **TODO 5 de Athena en AWS** (pendiente).

**Limitaciones:**
- **Estado acumulado**: cada corrida parte de las tablas/checkpoints de la anterior,
  así que los deltas de tabla mezclan el trabajo de distintos envíos.
- **Ventanas pendientes**: ventanas cuyo watermark no se cerró dentro de la ventana en
  vivo pueden emitirse en el drenaje o quedar pendientes para una corrida futura.
- **Eventos pendientes en el topic**: `bronze` no drena (solo las consultas aguas
  abajo lo hacen), así que al cerrar su ventana deja eventos sin consumir (ver §5).
- IDs nuevos por ejecución (sin `--seed`): no hay solapamiento de `transaction_id`
  entre envíos, así que los +5 752 son transacciones nuevas, no reprocesos.

## 4. Experimento de partition key (topics de experimento, 3 particiones, rate 20 duration 60 --fast)

Topics nuevos y exclusivos (no consumidos por el pipeline principal):
`paytrack.exp.customer`, `paytrack.exp.constant`, `paytrack.exp.transaction`.
Offsets por partición = nº de mensajes (los topics arrancaron vacíos). Evidencia: `07d`.

| Llave | Enviados | P0 | P1 | P2 | % P0 / P1 / P2 |
|---|---|---|---|---|---|
| `customer_id` (`07a`) | 1 207 | 460 | 339 | 408 | 38,1 / 28,1 / 33,8 |
| constante `"CONST"` (`07b`) | 1 230 | 1 230 | 0 | 0 | 100 / 0 / 0 |
| `transaction_id` (`07c`) | 1 224 | 391 | 429 | 404 | 31,9 / 35,0 / 33,0 |

**Interpretación (comprobable):**
- **customer_id**: reparte entre las 3 particiones (no uniforme: hay ráfagas y
  contracargos del mismo cliente que caen juntos). Todos los eventos de un cliente
  (pagos, ráfagas y el contracargo de su pago) se **asignan a la misma partición**.
  Esto fija **a qué partición va cada evento** y preserva el **orden de llegada
  (offset) dentro de esa partición**. **No** garantiza orden **cronológico por
  `event_time`**: los eventos tardíos llegan con un `event_time` anterior pero se
  colocan en el offset de su llegada, no en orden de `event_time`. (El orden temporal
  real lo recupera el watermark/las ventanas sobre `event_time`, no la partition key.)
- **constante**: todo a una sola partición (0 %/0 % en las otras) → sin paralelismo,
  un solo consumidor efectivo; es el anti-patrón a evitar.
- **transaction_id**: el reparto más parejo de los tres (cada transacción tiene su
  propio id). Matiz importante sobre el orden: el contracargo **comparte el mismo
  `transaction_id`** que su pago (el productor hace `{**txn, "status": "chargeback"}`),
  así que con esta llave el **par pago↔contracargo sí cae en la misma partición** y
  queda ordenado. Lo que **se dispersa** con `transaction_id` es el conjunto de
  eventos **del mismo cliente** (sus varios pagos y sus ráfagas), que con
  `customer_id` iban juntos y ahora quedan repartidos entre particiones.

**Precisión de las alertas con `transaction_id` (hipótesis + diseño):** Spark agrupa
las alertas **por `customer_id`** con ventanas de tiempo sobre `event_time`,
independientemente de la partición de Kafka. Por eso el **número de alertas que
calcula Spark no debería cambiar** por la partition key: la agregación no depende del
reparto físico. La partition key importa para el **orden por partición** y el
**reparto de carga/paralelismo**, no para el resultado de una agregación global por
cliente. (No se re-ejecutó el pipeline sobre estos topics de experimento para
confirmarlo empíricamente: es análisis de diseño.)

**Lo que la partition key NO hace (aclaraciones pedidas):**
- **No** garantiza orden **global**: Kafka solo ordena por **offset dentro** de una
  partición.
- **No** garantiza orden **cronológico por `event_time`**: solo mantiene juntos en una
  partición los eventos de una misma llave, en su orden de **llegada**.
- **No** resuelve los **reintentos parciales de Kinesis** (`put_with_retries`): eso lo
  maneja el reenvío de los registros fallidos, no la llave.

**Restauración:** `partition_key` se devolvió exactamente a
`return str(event["customer_id"])` tras el experimento (los topics de experimento se
conservan, no se borran).

## 5. Conciliación de contadores (qué mide cada uno)

Cada número mide una cosa distinta; **no son intercambiables ni sumables entre
etapas**:

| Contador | Qué mide | Dónde |
|---|---|---|
| "eventos enviados" | registros que el **productor** publicó en Kafka | `03a`/`05b`/`06c` |
| offsets del topic | registros **retenidos** en el topic (acumulado de todos los envíos) | lectura `kafka-get-offsets` |
| `bronze.numInputRows` | eventos que **bronze leyó de Kafka** en esa corrida | `report()` |
| Δ tabla `bronze` | eventos **persistidos** en Delta bronze en esa corrida | `06a`/`06d` etc. |
| `silver/metrics/alerts.numInputRows` | filas leídas **de la tabla bronze** (no de Kafka) | `report()` |
| Δ tabla `silver` | `transaction_id` **distintos insertados** (neto del MERGE) | conteos antes/después |

**Cadena comprobada (lectura de solo lectura del topic, `04c`/`06a`/`06d`):**
- Enviados en total: 12 316 + 2 443 + 6 207 = **20 966**, igual al total del topic
  (8 053 + 6 346 + 6 567 = **20 966**). → el topic retiene **todos** los eventos.
- Bronze persistió: availableNow 12 316 → +659 (rate 20) = 12 975 → +6 074 (rate 50)
  = **19 049** (coincide con la tabla bronze en `06d`).
- **Pendientes en el topic** (producidos pero aún no consumidos por bronze):
  tras rate 20 = 14 759 − 12 975 = **1 784**; tras rate 50 = 20 966 − 19 049 = **1 917**.
  (El "1 784" del resumen previo correspondía al estado **tras rate 20**, no a la
  resta de la corrida rate 50.)
- En la corrida rate 50: enviados **6 207**, persistidos por bronze **6 074**. El
  **133** (6 207 − 6 074) es el **aumento neto del atraso (backlog) entre el inicio y
  el fin de esa corrida**: la ventana en vivo de bronze se cerró antes de consumir
  todo lo que el productor publicó. **No es una atribución de eventos por origen**
  (no dice "133 eventos del rate 50 quedaron fuera" frente a los del rate 20): el
  atraso es una sola cola compartida en el topic. El atraso total pasó de **1 784**
  (tras rate 20) a **1 917** (tras rate 50), es decir +133 netos. Esto ocurre porque
  **bronze no drena al cerrar su ventana en vivo** (solo las consultas aguas abajo
  drenan).

**Corrección del error aritmético previo:** el resumen anterior escribió
"6 074 + 511 = 6 585" y luego "6 585 − 6 207 = 378". Eso **sumaba dos etapas
distintas** (bronze-desde-Kafka 6 074 + silver-desde-bronze 511) como si fueran
eventos nuevos. La cifra correcta de eventos que bronze persistió desde Kafka en esa
corrida es **6 074**, y la diferencia con lo enviado es **6 207 − 6 074 = 133**
(eventos pendientes de esa corrida), no 378.

## 6. Cierre de la validación local: procesamiento del atraso pendiente

Objetivo: consumir el atraso (**1 917** eventos) que quedó en el topic tras las
corridas en vivo, con `run_local.sh stream` (availableNow), **sin enviar eventos
nuevos**. Antes de empezar se comprobó que **no había productores ni jobs de streaming
activos** (solo el broker de Kafka en pie). Evidencias: `10a`–`10g`.

**Ejecución 1 — procesar pendientes (`10c`):**

| Contador | Antes (`10a`/`10b`) | Después (`10d`/`10e`) |
|---|---|---|
| Topic (offsets P0/P1/P2) | 8053 / 6346 / 6567 (= 20 966) | 8053 / 6346 / 6567 (= 20 966) |
| Offsets procesados por bronze (checkpoint) | — | 8053 / 6346 / 6567 (= 20 966) |
| Tabla bronze | 19 049 | **20 966** (+1 917) |
| Tabla silver | 46 341 | **48 169** (+1 828) |
| Ventanas metrics | 170 | **178** (+8) |
| Alertas | 201 | **201** (+0) |

- `report()` de esta corrida: bronze **1 917** filas leídas (= el atraso exacto),
  silver 1 917, metrics/alerts 1 917 leídas con **35** descartes por watermark cada
  una. **Todas las validaciones del job en ✓.**
- **Comprobado**: el commit del checkpoint de bronze (`10d`) quedó en
  `{P0:8053, P1:6346, P2:6567}`, idéntico a los offsets del topic → bronze consumió
  **todo** el topic; ya **no hay eventos pendientes de lectura**. Bronze llegó a
  **20 966**, como se esperaba según los conteos anteriores.
- Las **+8 ventanas de métricas y las 0 alertas nuevas** son **procesamiento posterior
  acumulado** del atraso (eventos de todas las corridas previas que faltaban por
  consumir). **No se atribuyen exclusivamente al rate 50**: el atraso era una cola
  mezclada de los tres envíos. Que las alertas no aumentaran (+0) es consistente con
  que las ráfagas ya estaban mayormente contabilizadas y/o con ventanas de alerta que
  **siguen abiertas** esperando que avance el watermark (ver nota abajo).

**Ejecución 2 — idempotencia (`10f`/`10g`), sin eventos nuevos:**
- Las **cuatro** consultas leyeron **0 filas** (un micro-lote vacío cada una), 0
  descartes por watermark. Todas las validaciones en ✓.
- Conteos **idénticos** a la ejecución 1 (bronze 20 966, silver 48 169, metrics 178,
  alerts 201); `diff 10e vs 10g` vacío. Los checkpoints recordaron el avance → no se
  reprocesó nada.

**Nota (pendiente de lectura ≠ ventana abierta):** tras la ejecución 1 **no quedan
eventos pendientes de lectura** (el checkpoint de bronze = offsets del topic). Que no
aparezcan más alertas o métricas no implica atraso de lectura: algunas **ventanas
pueden seguir abiertas** hasta que el watermark avance lo suficiente para emitirlas, y
eso no se fuerza. Son dos cosas distintas: *backlog de ingesta* (resuelto) vs
*ventanas aún no cerradas* (comportamiento normal del modo append + watermark).

## Mediciones pendientes para AWS
- **Precisión de alertas (TODO 5 de Athena)** a 20 y 50 eventos/s: no medible en local.
- Comparación de MB escaneados (TODO 1 vs 2 de Athena).
- Ejecución de los jobs de Glue (batch y streaming) y verificación en CloudWatch.

## Pruebas adicionales que exigiría el laboratorio (no ejecutadas aquí)
- **Desglose del +621 / del "38"**: queda como **no determinado**. Si se quisiera
  descomponer, se podría registrar `numTargetRowsInserted/Updated` por micro-lote del
  MERGE o contar los eventos por `status`/`event_id`; pero **no es un requisito del
  laboratorio** ni imprescindible para cerrar la validación local.
- **rate 20 vs 50 controlado**: para una comparación limpia habría que partir de
  `lakehouse-local` y topic nuevos (o checkpoints frescos) en cada rate, garantizando
  el mismo solapamiento productor↔ventana. No se ejecuta en esta parte.
- **Precisión de alertas**: solo medible con el TODO 5 de Athena en AWS.
