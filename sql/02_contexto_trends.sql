-- =============================================================
-- Vertical II - Contexto de demanda digital por distrito (Google Trends)
-- Cada distrito ficticio de DataLandia se asocia a una region proxy de Peru
-- (columna region_trends_patron de gold_dim_distrito_v1).
--
-- PASO PREVIO (revisar nombres exactos de regiones y semanas disponibles):
--   SELECT DISTINCT region_name
--   FROM `bigquery-public-data.google_trends.international_top_terms`
--   WHERE refresh_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
--     AND country_code = 'PE'
--   ORDER BY 1;
-- Si una region no hace match, corrige el patron en dim_distrito (job Spark 2)
-- o con un UPDATE sobre gold_dim_distrito_v1.
-- Filtrar SIEMPRE por refresh_date: la tabla es grande y esta particionada por esa columna.
-- =============================================================

CREATE OR REPLACE TABLE lumina_gold.gold_contexto_trends_distrito_v1 AS
WITH t AS (
  SELECT week, region_name, term, score, rank
  FROM `bigquery-public-data.google_trends.international_top_terms`
  WHERE refresh_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND country_code = 'PE'
    AND week BETWEEN DATE '2026-08-23' AND DATE '2026-10-10'
)
SELECT
  d.distrito_id,
  d.distrito_nombre,
  t.week AS semana,
  COUNT(DISTINCT t.term) AS n_terminos_tendencia,
  ROUND(AVG(t.score), 2) AS trends_score_promedio,
  COUNT(DISTINCT IF(REGEXP_CONTAINS(LOWER(t.term),
        r'(banco|bcp|interbank|bbva|scotiabank|yape|plin|prestamo|préstamo|credito|crédito|ahorro|bono|afp|cts|gratificaci|sunat|dolar|dólar)'),
        t.term, NULL)) AS n_terminos_financieros
FROM t
JOIN lumina_gold.gold_dim_distrito_v1 d
  ON REGEXP_CONTAINS(LOWER(t.region_name), d.region_trends_patron)
GROUP BY 1, 2, 3;

SELECT * FROM lumina_gold.gold_contexto_trends_distrito_v1 ORDER BY semana, distrito_id;
