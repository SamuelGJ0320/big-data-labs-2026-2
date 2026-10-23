-- Q1: Monto total por categoría de comercio (últimos 30 días)
EXPLAIN ANALYZE
SELECT
    m.category,
    COUNT(*)          AS num_transactions,
    SUM(f.amount_cop) AS total_cop,
    AVG(f.amount_cop) AS avg_cop
FROM dw.fact_transactions f
JOIN dw.dim_merchant m
    ON f.merchant_key = m.merchant_key
JOIN dw.dim_date d
    ON f.date_key = d.date_key
WHERE d.full_date >= CURRENT_DATE - 30
  AND f.is_approved
GROUP BY m.category
ORDER BY total_cop DESC;

-- Q2: Tasa de fraude por segmento actual del cliente
-- La versión asociada al hecho identifica al cliente.
-- La versión vigente aporta su segmento actual, como en el OLTP.
EXPLAIN ANALYZE
SELECT
    actual.segment,
    COUNT(*) AS total_txn,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_txn,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END)
        / COUNT(*),
        2
    ) AS fraud_rate_pct
FROM dw.fact_transactions f
JOIN dw.dim_customer historico
    ON f.customer_key = historico.customer_key
JOIN dw.dim_customer actual
    ON historico.customer_id = actual.customer_id
   AND actual.is_current
GROUP BY actual.segment
ORDER BY fraud_rate_pct DESC;

-- Q3: Volumen por día de la semana y hora (últimos 7 días)
EXPLAIN ANALYZE
SELECT
    d.day_of_week,
    h.hour,
    COUNT(*)          AS num_transactions,
    SUM(f.amount_cop) AS total_cop
FROM dw.fact_transactions f
JOIN dw.dim_date d
    ON f.date_key = d.date_key
JOIN dw.dim_time h
    ON f.time_key = h.time_key
WHERE f.is_approved
  AND d.full_date >= CURRENT_DATE - 7
GROUP BY d.day_of_week, h.hour
ORDER BY d.day_of_week, h.hour;