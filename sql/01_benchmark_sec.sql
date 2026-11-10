-- =============================================================
-- Vertical III - Benchmark de la banca tradicional (SEC, dataset publico)
-- PASO PREVIO: revisa los nombres reales de las columnas con
--   SELECT column_name, data_type
--   FROM `bigquery-public-data.sec_quarterly_financials.INFORMATION_SCHEMA.COLUMNS`
--   WHERE table_name = 'quick_summary';
-- Si alguna columna se llama distinto, ajustala abajo.
-- =============================================================

CREATE OR REPLACE TABLE lumina_gold.gold_sec_bancos_anual_v1 AS
WITH base AS (
  SELECT company_name, CAST(sic AS STRING) AS sic, fiscal_year, measure_tag, value
  FROM `bigquery-public-data.sec_quarterly_financials.quick_summary`
  WHERE CAST(sic AS STRING) IN ('6021', '6022', '6029', '6035', '6036')   -- bancos comerciales y de ahorro
    AND form = '10-K'
    AND measure_tag IN ('Assets', 'StockholdersEquity', 'NetIncomeLoss',
                        'InterestIncomeExpenseNet', 'NoninterestIncome', 'NoninterestExpense')
    AND ( (measure_tag IN ('Assets', 'StockholdersEquity') AND number_of_quarters = 0)
          OR number_of_quarters = 4 )
)
SELECT company_name, sic, fiscal_year,
  MAX(IF(measure_tag = 'Assets', value, NULL))                   AS activos,
  MAX(IF(measure_tag = 'StockholdersEquity', value, NULL))       AS patrimonio,
  MAX(IF(measure_tag = 'NetIncomeLoss', value, NULL))            AS utilidad_neta,
  MAX(IF(measure_tag = 'InterestIncomeExpenseNet', value, NULL)) AS ingreso_neto_intereses,
  MAX(IF(measure_tag = 'NoninterestIncome', value, NULL))        AS ingreso_no_intereses,
  MAX(IF(measure_tag = 'NoninterestExpense', value, NULL))       AS gasto_operativo
FROM base
GROUP BY 1, 2, 3;

-- Indicadores del sector por anio (medianas, robustas a outliers)
CREATE OR REPLACE TABLE lumina_gold.gold_benchmark_sector_v1 AS
SELECT fiscal_year,
  COUNT(*) AS n_bancos,
  APPROX_QUANTILES(SAFE_DIVIDE(utilidad_neta, activos), 100)[OFFSET(50)]    AS roa_mediana,
  APPROX_QUANTILES(SAFE_DIVIDE(utilidad_neta, patrimonio), 100)[OFFSET(50)] AS roe_mediana,
  APPROX_QUANTILES(SAFE_DIVIDE(gasto_operativo,
      ingreso_neto_intereses + IFNULL(ingreso_no_intereses, 0)), 100)[OFFSET(50)] AS ratio_eficiencia_mediana
FROM lumina_gold.gold_sec_bancos_anual_v1
WHERE activos > 0
GROUP BY fiscal_year
HAVING n_bancos >= 20
ORDER BY fiscal_year;

-- NOTA DE COBERTURA: el dataset publico tiene cobertura completa hasta 2018
-- (400+ bancos por anio); 2019-2020 solo tienen ~25 bancos y no son representativos.
-- Por eso la referencia del sector se toma del anio mas reciente con n_bancos >= 100 (2018).
SELECT * FROM lumina_gold.gold_benchmark_sector_v1 ORDER BY fiscal_year DESC LIMIT 5;

-- Anio de referencia usado en los KPIs (sql/04_kpis_operativos.sql)
SELECT * FROM lumina_gold.gold_benchmark_sector_v1
WHERE n_bancos >= 100
QUALIFY ROW_NUMBER() OVER (ORDER BY fiscal_year DESC) = 1;
