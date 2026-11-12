-- =============================================================
-- Validacion del pipeline de tiempo real (correr con el producer activo)
-- =============================================================

-- 1) Decisiones, latencia y fraude real por decision
SELECT decision,
  COUNT(*) AS n_tx,
  ROUND(AVG(latencia_ms)) AS latencia_prom_ms,
  APPROX_QUANTILES(latencia_ms, 100)[OFFSET(95)] AS latencia_p95_ms,
  SUM(etiqueta_fraude) AS fraudes_reales
FROM lumina_rt.rt_transaccion_scored_v1
WHERE DATE(ts_evento) >= '2026-11-10'  -- fecha de la prueba de streaming
GROUP BY decision ORDER BY decision;

-- 2) Precision y recall de las reglas (REVISAR o BLOQUEAR = alerta)
SELECT
  ROUND(SAFE_DIVIDE(COUNTIF(decision != 'APROBAR' AND etiqueta_fraude = 1),
                    COUNTIF(etiqueta_fraude = 1)), 4) AS recall,
  ROUND(SAFE_DIVIDE(COUNTIF(decision != 'APROBAR' AND etiqueta_fraude = 1),
                    COUNTIF(decision != 'APROBAR')), 4) AS precision_alertas
FROM lumina_rt.rt_transaccion_scored_v1
WHERE DATE(ts_evento) >= '2026-11-10';  -- fecha de la prueba de streaming

-- 3) Agregados por minuto (alimentan el tablero operativo)
SELECT * FROM lumina_rt.rt_agg_minuto_v1
ORDER BY ventana_inicio DESC, distrito_id LIMIT 40;

-- 4) KPI "tiempo promedio de aprobacion" (Vertical III) desde el flujo real
CREATE OR REPLACE VIEW lumina_gold.vw_tiempo_aprobacion_v1 AS
SELECT DATE(ts_evento) AS fecha, decision,
  COUNT(*) AS n_tx,
  ROUND(AVG(latencia_ms)) AS latencia_prom_ms,
  APPROX_QUANTILES(latencia_ms, 100)[OFFSET(95)] AS latencia_p95_ms
FROM lumina_rt.rt_transaccion_scored_v1
GROUP BY 1, 2;
