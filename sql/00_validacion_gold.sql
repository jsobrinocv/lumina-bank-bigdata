-- =============================================================
-- Validaciones del modelo estrella (evidencias para la entrega)
-- =============================================================

-- 1) Conteos por tabla
SELECT 'fact_transaccion' AS tabla, COUNT(*) AS filas FROM lumina_gold.gold_fact_transaccion_v1
UNION ALL SELECT 'dim_cliente', COUNT(*) FROM lumina_gold.gold_dim_cliente_v1
UNION ALL SELECT 'dim_fecha', COUNT(*) FROM lumina_gold.gold_dim_fecha_v1
UNION ALL SELECT 'dim_distrito', COUNT(*) FROM lumina_gold.gold_dim_distrito_v1
UNION ALL SELECT 'dim_tipo_transaccion', COUNT(*) FROM lumina_gold.gold_dim_tipo_transaccion_v1;

-- 2) Integridad referencial: hechos sin cliente / sin fecha (debe dar 0)
SELECT
  COUNTIF(c.cliente_id IS NULL) AS hechos_sin_cliente,
  COUNTIF(d.fecha IS NULL)      AS hechos_sin_fecha
FROM lumina_gold.gold_fact_transaccion_v1 f
LEFT JOIN lumina_gold.gold_dim_cliente_v1 c ON f.cliente_origen_id = c.cliente_id
LEFT JOIN lumina_gold.gold_dim_fecha_v1  d ON f.fecha = d.fecha;

-- 3) Unicidad de la llave de hechos (debe dar 0)
SELECT COUNT(*) - COUNT(DISTINCT transaccion_id) AS ids_duplicados
FROM lumina_gold.gold_fact_transaccion_v1;

-- 4) Consulta OLAP de ejemplo: volumen y fraude por distrito, segmento y canal
SELECT ds.distrito_nombre, c.segmento_etario, f.canal,
       COUNT(*) AS n_tx, ROUND(SUM(f.monto), 0) AS monto_total,
       ROUND(100 * AVG(f.es_fraude), 3) AS pct_fraude
FROM lumina_gold.gold_fact_transaccion_v1 f
JOIN lumina_gold.gold_dim_cliente_v1  c  ON f.cliente_origen_id = c.cliente_id
JOIN lumina_gold.gold_dim_distrito_v1 ds ON f.distrito_id = ds.distrito_id
GROUP BY 1, 2, 3
ORDER BY monto_total DESC
LIMIT 20;
