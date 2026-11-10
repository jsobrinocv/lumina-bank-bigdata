-- =============================================================
-- Vertical III - Informes operativos bajo demanda
-- Reemplaza el costo diario con el valor de la calculadora de la Entrega 1.
-- =============================================================
DECLARE costo_plataforma_diario_usd FLOAT64 DEFAULT 120.0;  -- SUPUESTO: ajustar

-- KPI diario del sistema
CREATE OR REPLACE TABLE lumina_gold.gold_kpi_operativo_diario_v1
PARTITION BY fecha AS
SELECT
  f.fecha,
  COUNT(*) AS n_transacciones,
  ROUND(SUM(f.monto), 2) AS monto_total,
  ROUND(SUM(f.monto * t.comision_pct), 2) AS ingresos_estimados,
  ROUND(costo_plataforma_diario_usd / COUNT(*), 6) AS costo_operativo_por_tx,
  ROUND(SAFE_DIVIDE(COUNTIF(f.es_fraude = 1), COUNT(*)), 6) AS tasa_fraude,
  ROUND(SUM(IF(f.es_fraude = 1, f.monto, 0)), 2) AS monto_fraude,
  ROUND(SAFE_DIVIDE(SUM(IF(f.es_fraude = 1, f.monto, 0)), SUM(f.monto)), 6) AS tasa_perdida_fraude,
  ROUND(SAFE_DIVIDE(COUNTIF(f.es_fraude = 1 AND f.alerta_sistema_legado = 1),
                    COUNTIF(f.es_fraude = 1)), 4) AS recall_sistema_legado
FROM lumina_gold.gold_fact_transaccion_v1 f
JOIN lumina_gold.gold_dim_tipo_transaccion_v1 t USING (tipo)
GROUP BY f.fecha;

-- Ingresos por producto financiero
CREATE OR REPLACE TABLE lumina_gold.gold_ingresos_producto_diario_v1
PARTITION BY fecha AS
SELECT f.fecha, t.producto,
  COUNT(*) AS n_transacciones,
  ROUND(SUM(f.monto), 2) AS monto,
  ROUND(SUM(f.monto * t.comision_pct), 2) AS ingresos_estimados
FROM lumina_gold.gold_fact_transaccion_v1 f
JOIN lumina_gold.gold_dim_tipo_transaccion_v1 t USING (tipo)
GROUP BY 1, 2;

-- Salud financiera: Lumina vs banca tradicional (ultimo anio disponible en SEC)
CREATE OR REPLACE VIEW lumina_gold.vw_salud_lumina_vs_sector_v1 AS
WITH lumina AS (
  SELECT
    SAFE_DIVIDE(SUM(n_transacciones * costo_operativo_por_tx), SUM(ingresos_estimados)) AS ratio_eficiencia,
    SAFE_DIVIDE(SUM(monto_fraude), SUM(monto_total)) AS tasa_perdida_fraude,
    AVG(costo_operativo_por_tx) AS costo_por_tx
  FROM lumina_gold.gold_kpi_operativo_diario_v1
),
sector AS (
  SELECT * FROM lumina_gold.gold_benchmark_sector_v1
    WHERE n_bancos >= 100   -- anio mas reciente con muestra representativa (2018)
  QUALIFY ROW_NUMBER() OVER (ORDER BY fiscal_year DESC) = 1
)
SELECT
  'Ratio de eficiencia (costo / ingreso)' AS indicador,
  ROUND(l.ratio_eficiencia, 4) AS lumina_bank,
  ROUND(s.ratio_eficiencia_mediana, 4) AS banca_tradicional,
  s.fiscal_year AS anio_referencia_sector
FROM lumina l CROSS JOIN sector s
UNION ALL
SELECT 'ROA mediano del sector', NULL, ROUND(s.roa_mediana, 4), s.fiscal_year
FROM sector s
UNION ALL
SELECT 'Tasa de perdida por fraude (monto)', ROUND(l.tasa_perdida_fraude, 6), NULL, NULL
FROM lumina l;

SELECT * FROM lumina_gold.gold_kpi_operativo_diario_v1 ORDER BY fecha LIMIT 10;
