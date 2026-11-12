-- =============================================================
-- Gobierno de datos - Reglas de calidad sobre la tabla de hechos
-- Mismas 6 reglas definidas en el analisis de calidad de Knowledge Catalog.
-- Se guarda una fila por regla y ejecucion (historial auditable).
-- =============================================================
CREATE TABLE IF NOT EXISTS lumina_gold.gold_dq_resultados_v1 (
  ts_ejecucion TIMESTAMP, tabla STRING, regla STRING, dimension STRING,
  total_filas INT64, filas_incumplen INT64, pct_cumplimiento FLOAT64, resultado STRING
);

INSERT INTO lumina_gold.gold_dq_resultados_v1
WITH m AS (
  SELECT
    COUNT(*) AS total,
    COUNTIF(transaccion_id IS NULL) AS id_nulo,
    COUNT(*) - COUNT(DISTINCT transaccion_id) AS id_duplicado,
    COUNTIF(monto < 0) AS monto_negativo,
    COUNTIF(tipo IS NULL OR tipo NOT IN ('CASH_IN','CASH_OUT','DEBIT','PAYMENT','TRANSFER')) AS tipo_invalido,
    COUNTIF(es_fraude IS NULL OR es_fraude NOT IN (0, 1)) AS fraude_invalido,
    COUNTIF(fecha IS NULL) AS fecha_nula
  FROM lumina_gold.gold_fact_transaccion_v1
)
SELECT CURRENT_TIMESTAMP(), 'gold_fact_transaccion_v1', r.regla, r.dimension, m.total, r.n,
       ROUND(100 * (1 - SAFE_DIVIDE(r.n, m.total)), 4),
       IF(r.n = 0, 'APROBADA', 'FALLIDA')
FROM m, UNNEST([
  STRUCT('transaccion_id no nulo' AS regla, 'Completitud' AS dimension, id_nulo AS n),
  STRUCT('transaccion_id unico', 'Unicidad', id_duplicado),
  STRUCT('monto >= 0', 'Validez', monto_negativo),
  STRUCT('tipo en conjunto valido', 'Validez', tipo_invalido),
  STRUCT('es_fraude en {0,1}', 'Validez', fraude_invalido),
  STRUCT('fecha no nula', 'Completitud', fecha_nula)
]) AS r;

-- Resultado de la ultima ejecucion
SELECT regla, dimension, total_filas, filas_incumplen, pct_cumplimiento, resultado
FROM lumina_gold.gold_dq_resultados_v1
QUALIFY ts_ejecucion = MAX(ts_ejecucion) OVER ()
ORDER BY dimension, regla;
