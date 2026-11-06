-- ============================================================
-- Consumo: consultas Athena sobre las tablas Delta del lakehouse
--
-- Ejecútalas en la consola de Athena con el workgroup paytrack-lakehouse-wg.
-- Reemplaza paytrack-lakehouse-sgutierr28-918897411141 por el nombre de tu bucket (salida del deploy).
-- Anota los MB escaneados de cada consulta (pestaña "Query stats").
-- ============================================================

-- ── Dado: registrar las tablas Delta en el catálogo ─────────
-- Athena lee el esquema y las particiones directamente del _delta_log.
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.silver_transactions
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/silver/transactions/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_daily_merchant_summary
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/gold/daily_merchant_summary/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_weekly_report
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/gold/fraud_weekly_report/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_hourly_volume
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/gold/hourly_volume/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_status_changes
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/gold/status_changes/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Tablas del streaming (parte B)
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_merchant_minute_metrics
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/gold/merchant_minute_metrics/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_alerts
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/gold/fraud_alerts/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.bronze_transaction_events
LOCATION 's3://paytrack-lakehouse-sgutierr28-918897411141/lakehouse/bronze/transaction_events/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- ── TODO 1 ──────────────────────────────────────────────────
-- Los 5 comercios con mayor monto aprobado en septiembre de 2026 y su tasa
-- de aprobación del mes, usando gold_daily_merchant_summary.
-- Ojo: no promedies approval_rate_pct diaria; calcula la del mes.
-- Filtra por las columnas de partición.


-- ── TODO 2 ──────────────────────────────────────────────────
-- La misma pregunta del TODO 1, pero calculada desde silver_transactions.
-- Compara resultados y MB escaneados con el TODO 1.


-- ── TODO 3 ──────────────────────────────────────────────────
-- Para cada segmento de cliente: la semana con más eventos de fraude y
-- cuántos eventos tuvo (gold_fraud_weekly_report).
-- Pista: funciones de ventana (ROW_NUMBER ... OVER (PARTITION BY ...)).


-- ── TODO 4 ──────────────────────────────────────────────────
-- Impacto de los contracargos del último lote: número de transacciones y
-- monto revertido por categoría de comercio (gold_status_changes + silver_transactions).


-- ── TODO 5 (streaming) ──────────────────────────────────────
-- Precisión de las alertas de ráfagas: de las alertas de gold_fraud_alerts,
-- ¿qué porcentaje corresponde a clientes que de verdad cometieron fraude en
-- esa ventana? Una alerta es un acierto si alguno de los pagos del cliente con
-- event_time dentro de [window_start, window_end) tiene is_fraud = true
-- (bronze_transaction_events). Reporta alertas, aciertos y % de precisión.
--
-- Se marca cada alerta como acierto UNA sola vez (MAX sobre el LEFT JOIN, por
-- la llave de la alerta: window_start, window_end, customer_id), para no
-- multiplicar la alerta por cada pago que cae en la ventana. El LEFT JOIN y el
-- NULLIF manejan el caso sin alertas / sin pagos coincidentes (precisión NULL si
-- no hay alertas, en vez de dividir por cero).
WITH alert_hits AS (
    SELECT
        a.window_start,
        a.window_end,
        a.customer_id,
        MAX(CASE
                WHEN b.is_fraud = true
                 AND b.event_type = 'payment'
                 AND b.event_time >= a.window_start
                 AND b.event_time <  a.window_end
                THEN 1 ELSE 0
            END) AS is_hit
    FROM paytrack_lakehouse.gold_fraud_alerts a
    LEFT JOIN paytrack_lakehouse.bronze_transaction_events b
           ON b.customer_id = a.customer_id
          AND b.event_time >= a.window_start
          AND b.event_time <  a.window_end
    GROUP BY a.window_start, a.window_end, a.customer_id
)
SELECT
    COUNT(*)                                                   AS total_alertas,
    SUM(is_hit)                                                AS aciertos,
    ROUND(100.0 * SUM(is_hit) / NULLIF(COUNT(*), 0), 2)        AS precision_pct
FROM alert_hits;


-- ── TODO 6 (streaming) ──────────────────────────────────────
-- Capa rápida vs. capa batch: para cada día que cubre el streaming, compara el
-- número de pagos por comercio según gold_merchant_minute_metrics (suma de las
-- ventanas) con el número de transacciones del stream en silver_transactions
-- (las que tienen transaction_id con prefijo 'TXNS'). Muestra la diferencia.
-- ¿De dónde sale? (pista: watermark y ventanas que aún no se cierran)
--
-- Grano: una fila por (día, comercio). La capa rápida agrega por el día de
-- window_start (métricas por minuto ya cerradas por el watermark); la capa batch
-- cuenta transacciones del stream por el día de event_ts. FULL OUTER JOIN para no
-- perder comercios/días que aparezcan en una capa pero no en la otra.
WITH fast_layer AS (
    SELECT
        date(window_start)      AS dia,
        merchant_id,
        SUM(num_payments)       AS pagos_ventanas
    FROM paytrack_lakehouse.gold_merchant_minute_metrics
    GROUP BY date(window_start), merchant_id
),
batch_layer AS (
    SELECT
        date(event_ts)          AS dia,
        merchant_id,
        COUNT(*)                AS txns_stream_silver
    FROM paytrack_lakehouse.silver_transactions
    WHERE transaction_id LIKE 'TXNS%'
    GROUP BY date(event_ts), merchant_id
)
SELECT
    COALESCE(f.dia, b.dia)                   AS dia,
    COALESCE(f.merchant_id, b.merchant_id)   AS merchant_id,
    COALESCE(f.pagos_ventanas, 0)            AS pagos_capa_rapida,
    COALESCE(b.txns_stream_silver, 0)        AS txns_capa_batch,
    COALESCE(b.txns_stream_silver, 0) - COALESCE(f.pagos_ventanas, 0) AS diferencia
FROM fast_layer f
FULL OUTER JOIN batch_layer b
  ON f.dia = b.dia
 AND f.merchant_id = b.merchant_id
ORDER BY dia, merchant_id;

