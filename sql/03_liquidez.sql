-- =============================================================
-- Vertical II - Plan diario de liquidez por distrito
-- Cruza: transacciones (PaySim) + calendario/eventos + Google Trends
-- y entrena un pronostico ARIMA_PLUS de retiros de efectivo (BigQuery ML).
-- =============================================================

CREATE OR REPLACE TABLE lumina_gold.gold_liquidez_diaria_v1
PARTITION BY fecha CLUSTER BY distrito_id AS
WITH diario AS (
  SELECT fecha, distrito_id,
    ROUND(SUM(IF(tipo = 'CASH_OUT', monto, 0)), 2) AS retiro_efectivo,
    ROUND(SUM(IF(tipo = 'CASH_IN',  monto, 0)), 2) AS deposito_efectivo,
    COUNT(*) AS n_transacciones,
    COUNTIF(canal IN ('CAJERO', 'AGENTE', 'OFICINA')) AS n_tx_canal_fisico
  FROM lumina_gold.gold_fact_transaccion_v1
  GROUP BY 1, 2
)
SELECT
  d.*,
  ROUND(d.retiro_efectivo - d.deposito_efectivo, 2) AS demanda_neta_efectivo,
  c.nombre_dia, c.es_fin_semana, c.es_dia_pago, c.es_cierre_mes, c.es_feriado, c.evento,
  tr.trends_score_promedio, tr.n_terminos_financieros,
  LAG(d.retiro_efectivo, 7) OVER w  AS retiro_mismo_dia_semana_anterior,
  LEAD(d.retiro_efectivo, 1) OVER w AS objetivo_retiro_dia_siguiente
FROM diario d
JOIN lumina_gold.gold_dim_fecha_v1 c USING (fecha)
LEFT JOIN lumina_gold.gold_contexto_trends_distrito_v1 tr
  ON tr.distrito_id = d.distrito_id
 AND tr.semana = DATE_TRUNC(d.fecha, WEEK(SUNDAY))
WINDOW w AS (PARTITION BY d.distrito_id ORDER BY d.fecha);

-- Modelo de pronostico (una serie por distrito)
CREATE OR REPLACE MODEL lumina_gold.ml_pronostico_efectivo_v1
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'fecha',
  time_series_data_col = 'retiro_efectivo',
  time_series_id_col = 'distrito_id',
  data_frequency = 'DAILY',
  horizon = 7,
  auto_arima = TRUE
) AS
SELECT fecha, distrito_id, retiro_efectivo
FROM lumina_gold.gold_liquidez_diaria_v1;

-- Plan de liquidez: efectivo a provisionar = banda alta del pronostico + 10% de colchon
CREATE OR REPLACE TABLE lumina_gold.gold_plan_liquidez_v1 AS
SELECT
  f.distrito_id,
  ds.distrito_nombre,
  DATE(f.forecast_timestamp) AS fecha,
  ROUND(f.forecast_value, 2) AS retiro_esperado,
  ROUND(f.prediction_interval_upper_bound, 2) AS retiro_escenario_alto,
  ROUND(f.prediction_interval_upper_bound * 1.10, 2) AS efectivo_a_provisionar
FROM ML.FORECAST(MODEL lumina_gold.ml_pronostico_efectivo_v1,
                 STRUCT(7 AS horizon, 0.9 AS confidence_level)) f
JOIN lumina_gold.gold_dim_distrito_v1 ds USING (distrito_id);

SELECT * FROM lumina_gold.gold_plan_liquidez_v1 ORDER BY fecha, distrito_id;
