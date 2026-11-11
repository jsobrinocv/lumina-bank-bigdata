-- =============================================================
-- Vertical II - Plan diario de liquidez por distrito
-- Cruza: transacciones (PaySim) + calendario/eventos + Google Trends
-- y entrena un pronostico ARIMA_PLUS de retiros de efectivo (BigQuery ML).
--
-- AJUSTE: PaySim concentra su volumen en los primeros ~17 dias; desde el
-- 18-set el volumen cae >90 % (artefacto del dataset sintetico). Para no
-- proyectar esa caida como tendencia (daba pronosticos negativos), el modelo
-- se entrena solo con los dias de operacion normal (>= 30 % del volumen
-- maximo diario) y en escala logaritmica (LN / EXP) para que el pronostico
-- nunca sea negativo.
-- =============================================================

-- 1) Base diaria por distrito: efectivo + calendario + contexto digital
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

-- 2) Modelo de pronostico (una serie por distrito)
--    Solo dias de operacion normal y retiros en escala logaritmica
CREATE OR REPLACE MODEL lumina_gold.ml_pronostico_efectivo_v1
OPTIONS (
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'fecha',
  time_series_data_col = 'log_retiro',
  time_series_id_col = 'distrito_id',
  data_frequency = 'DAILY',
  horizon = 7,
  auto_arima = TRUE
) AS
WITH vol AS (
  SELECT fecha, SUM(n_transacciones) AS n
  FROM lumina_gold.gold_liquidez_diaria_v1
  GROUP BY fecha
),
umbral AS (SELECT 0.3 * MAX(n) AS u FROM vol)
SELECT l.fecha, l.distrito_id, LN(1 + l.retiro_efectivo) AS log_retiro
FROM lumina_gold.gold_liquidez_diaria_v1 l
WHERE l.fecha IN (SELECT fecha FROM vol, umbral WHERE n >= u);

-- 3) Plan de liquidez: se vuelve a la escala original con EXP()
--    efectivo a provisionar = banda alta del pronostico (90 %) + 10 % de colchon
CREATE OR REPLACE TABLE lumina_gold.gold_plan_liquidez_v1 AS
SELECT
  f.distrito_id,
  ds.distrito_nombre,
  DATE(f.forecast_timestamp) AS fecha,
  ROUND(EXP(f.forecast_value) - 1, 2) AS retiro_esperado,
  ROUND(EXP(f.prediction_interval_upper_bound) - 1, 2) AS retiro_escenario_alto,
  ROUND((EXP(f.prediction_interval_upper_bound) - 1) * 1.10, 2) AS efectivo_a_provisionar
FROM ML.FORECAST(MODEL lumina_gold.ml_pronostico_efectivo_v1,
                 STRUCT(7 AS horizon, 0.9 AS confidence_level)) f
JOIN lumina_gold.gold_dim_distrito_v1 ds USING (distrito_id);

-- 4) Resultado
SELECT * FROM lumina_gold.gold_plan_liquidez_v1 ORDER BY fecha, distrito_id;
