-- ============================================================
-- Consumo: consultas Athena sobre las tablas Delta del lakehouse
--
-- Ejecútalas en la consola de Athena con el workgroup paytrack-lakehouse-wg.
-- Reemplaza <BUCKET> por el nombre de tu bucket (salida del deploy).
-- Anota los MB escaneados de cada consulta (pestaña "Query stats").
-- ============================================================

-- ── Dado: registrar las tablas Delta en el catálogo ─────────
-- Athena lee el esquema y las particiones directamente del _delta_log.
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.silver_transactions
LOCATION 's3://<BUCKET>/lakehouse/silver/transactions/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_daily_merchant_summary
LOCATION 's3://<BUCKET>/lakehouse/gold/daily_merchant_summary/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_weekly_report
LOCATION 's3://<BUCKET>/lakehouse/gold/fraud_weekly_report/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_hourly_volume
LOCATION 's3://<BUCKET>/lakehouse/gold/hourly_volume/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_status_changes
LOCATION 's3://<BUCKET>/lakehouse/gold/status_changes/'
TBLPROPERTIES ('table_type' = 'DELTA');

-- Tablas del streaming (parte B)
CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_merchant_minute_metrics
LOCATION 's3://<BUCKET>/lakehouse/gold/merchant_minute_metrics/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.gold_fraud_alerts
LOCATION 's3://<BUCKET>/lakehouse/gold/fraud_alerts/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS paytrack_lakehouse.bronze_transaction_events
LOCATION 's3://<BUCKET>/lakehouse/bronze/transaction_events/'
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


-- ── TODO 6 (streaming) ──────────────────────────────────────
-- Capa rápida vs. capa batch: para cada día que cubre el streaming, compara el
-- número de pagos por comercio según gold_merchant_minute_metrics (suma de las
-- ventanas) con el número de transacciones del stream en silver_transactions
-- (las que tienen transaction_id con prefijo 'TXNS'). Muestra la diferencia.
-- ¿De dónde sale? (pista: watermark y ventanas que aún no se cierran)

