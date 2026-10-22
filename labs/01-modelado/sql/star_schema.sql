-- ============================================================
-- PayTrack Star Schema (OLAP)
-- Esquema estrella para análisis de transacciones
--
-- Las dimensiones de calendario (dim_date, dim_time) están dadas.
-- Tú diseñas el resto: dimensiones de negocio, tabla de hechos e índices.
--
-- Contrato mínimo (lo usan load_star.py y check_queries.py):
--   * Cada dimensión tiene una surrogate key llamada <dimensión>_key
--     (customer_key, merchant_key, payment_method_key) y conserva su
--     natural key del OLTP (customer_id, merchant_id, method_name).
--   * dim_payment_method tiene la columna method_type.
--   * dim_customer es SCD Tipo 2 con valid_from, valid_to e is_current.
--   * fact_transactions tiene transaction_id, las 5 foreign keys
--     (date_key, time_key, customer_key, merchant_key, payment_method_key)
--     y las medidas amount_cop, is_fraud, is_approved y fraud_risk_score.
-- Todo lo demás (tipos de dato, atributos descriptivos, restricciones,
-- índices) es decisión de diseño tuya y debes justificarlo en el informe.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ── Dimensiones de calendario (dadas) ──────────────────────

-- Dim: Fecha (granularidad: día)
CREATE TABLE dw.dim_date (
    date_key        INT          PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE         NOT NULL,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(20)  NOT NULL,
    week_of_year    SMALLINT     NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    day_of_week     SMALLINT     NOT NULL,     -- 0 = domingo … 6 = sábado
    day_name        VARCHAR(20)  NOT NULL,
    is_weekend      BOOLEAN      NOT NULL,
    is_holiday_co   BOOLEAN      NOT NULL DEFAULT FALSE
);

-- Dim: Hora (granularidad: minuto del día)
CREATE TABLE dw.dim_time (
    time_key        INT          PRIMARY KEY,  -- HHMM
    hour            SMALLINT     NOT NULL,
    minute          SMALLINT     NOT NULL,
    period_of_day   VARCHAR(20)  NOT NULL      -- morning, afternoon, evening, night
);

-- ── TODO 1: Dimensión cliente (SCD Tipo 2) ─────────────────
-- Grano: una fila por version valida de un cliente
-- NO incluye name ni email: son datos personales (PII), no deben entrar al data warehouse
-- La PK es la surrogate key porque con SCD2 un mismo customer_id aparece en varias filas (versiones).

CREATE TABLE dw.dim_customer (
    customer_key        INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         VARCHAR(10)     NOT NULL,   -- llave natural del OLTP
    city                VARCHAR(100),
    segment             VARCHAR(50),                -- mass, premium, high_value
    monthly_income_cop  NUMERIC(15,2),
    valid_from          TIMESTAMP       NOT NULL,   -- inicio de vigencia de la version
    valid_to            TIMESTAMP       NOT NULL,   -- 'infinity' mientras esta vigente
    is_current          BOOLEAN         NOT NULL    -- TRUE = version activa
);

-- Busqueda SCD2: localizar la version vigente de un cliente
CREATE INDEX idx_dim_customer_lookup
    ON dw.dim_customer (customer_id, valid_from, valid_to);



-- ── TODO 2: Dimensión comercio ─────────────────────────────
-- SCD Tipo 1 (se sobrescribe): los atributos del comercio son estables y no cambian el analsis historico.

CREATE TABLE dw.dim_merchant (
    merchant_key        INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    merchant_id         VARCHAR(10)     NOT NULL,   -- llave natural del OLTP
    name                VARCHAR(200)    NOT NULL,
    category            VARCHAR(100),               -- atributo clave para Q1
    city                VARCHAR(100)
);



-- ── TODO 3: Dimensión método de pago ───────────────────────
-- method_type es un atributo CONFORMADO que no existe en el OLTP:
-- se deriva en la carga (credit_card -> card, nequi -> digital_wallet, etc.)

CREATE TABLE dw.dim_payment_method (
    payment_method_key  INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    method_id           INT             NOT NULL,   -- llave natural del OLTP
    method_name         VARCHAR(50)     NOT NULL,   -- credit_card, nequi, pse...
    method_type         VARCHAR(30)     NOT NULL   -- card, digital_wallet, bank_transfer
);



-- ── TODO 4: Tabla de hechos ────────────────────────────────
-- Grano: una fila por transacción de PayTrack.

CREATE TABLE dw.fact_transactions (
    transaction_id      VARCHAR(20)     PRIMARY KEY,
    -- 5 llaves foraneas a las dimensiones
    date_key            INT             NOT NULL REFERENCES dw.dim_date (date_key),
    time_key            INT             NOT NULL REFERENCES dw.dim_time (time_key),
    customer_key        INT             NOT NULL REFERENCES dw.dim_customer (customer_key),
    merchant_key        INT             NOT NULL REFERENCES dw.dim_merchant (merchant_key),
    payment_method_key  INT             NOT NULL REFERENCES dw.dim_payment_method (payment_method_key),
    -- medidas
    amount_cop          NUMERIC(15,2)   NOT NULL,   -- aditiva
    is_fraud            BOOLEAN         NOT NULL,   -- aditiva (suma = conteo)
    is_approved         BOOLEAN         NOT NULL,   -- aditiva (suma = conteo)
    fraud_risk_score    NUMERIC(5,4)                -- no aditiva: NULL si no hay evento
);



-- ── TODO 5: Índices para las consultas analíticas ──────────
-- Basados en los filtros y JOIN de olap_queries-sql:

CREATE INDEX idx_fact_date          ON dw.fact_transactions (date_key);                     -- Q1 y Q3: filtro por fecha  
CREATE INDEX idx_fact_merchant      ON dw.fact_transactions (merchant_key);                 -- Q1: JOIN con categoria
CREATE INDEX idx_fact_customer      ON dw.fact_transactions (customer_key);                 -- Q2: JOIN con segmento
CREATE INDEX idx_fact_fraud         ON dw.fact_transactions (merchant_key) WHERE is_fraud;  -- parcial: analisis de fraude