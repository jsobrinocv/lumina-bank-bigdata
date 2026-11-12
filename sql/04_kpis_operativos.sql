-- =============================================================
-- Vertical III - Informes operativos bajo demanda
-- Costo diario tomado de la GCP Pricing Calculator (seccion 9 del informe):
-- US$ 1 727 / mes / 30 = US$ 57,57 por dia. Supuesto: 1 u.m. de PaySim = 1 USD.
-- =============================================================
DECLARE costo_plataforma_diario_usd FLOAT64 DEFAULT 57.57;  -- Pricing Calculator: 1 727 / 30

-- 1) KPI diario del sistema
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

-- 2) Ingresos por producto financiero
CREATE OR REPLACE TABLE lumina_gold.gold_ingresos_producto_diario_v1
PARTITION BY fecha AS
SELECT f.fecha, t.producto,
  COUNT(*) AS n_transacciones,
  ROUND(SUM(f.monto), 2) AS monto,
  ROUND(SUM(f.monto * t.comision_pct), 2) AS ingresos_estimados
FROM lumina_gold.gold_fact_transaccion_v1 f
JOIN lumina_gold.gold_dim_tipo_transaccion_v1 t USING (tipo)
GROUP BY 1, 2;

-- 3) Salud financiera: metas de Lumina frente a la banca tradicional (SEC)
--    PaySim no tiene activos, patrimonio ni gastos totales, por lo que no se
--    puede calcular el mismo indicador para Lumina; el sector define las METAS.
--    Se usa el anio mas reciente con muestra representativa (n_bancos >= 100 -> 2018).
CREATE OR REPLACE VIEW lumina_gold.vw_salud_lumina_vs_sector_v1 AS
WITH sector AS (
  SELECT * FROM lumina_gold.gold_benchmark_sector_v1
  WHERE n_bancos >= 100
  QUALIFY ROW_NUMBER() OVER (ORDER BY fiscal_year DESC) = 1
)
SELECT 'Ratio de eficiencia (gastos operativos / ingresos)' AS indicador,
       ROUND(ratio_eficiencia_mediana, 4) AS valor_banca_tradicional,
       'Menor o igual' AS meta_lumina, fiscal_year AS anio_referencia, n_bancos
FROM sector
UNION ALL
SELECT 'ROA (utilidad / activos)', ROUND(roa_mediana, 4), 'Mayor o igual', fiscal_year, n_bancos
FROM sector
UNION ALL
SELECT 'ROE (utilidad / patrimonio)', ROUND(roe_mediana, 4), 'Mayor o igual', fiscal_year, n_bancos
FROM sector;

-- 4) Resultados
SELECT * FROM lumina_gold.gold_kpi_operativo_diario_v1 ORDER BY fecha LIMIT 10;
SELECT * FROM lumina_gold.vw_salud_lumina_vs_sector_v1;
