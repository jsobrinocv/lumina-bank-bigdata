-- =============================================================
-- Vertical II - Contexto de demanda digital por distrito (Google Trends)
-- El dataset publico international_top_terms NO incluye Peru.
-- Se usa Colombia (CO) como proxy: cada distrito ficticio de DataLandia
-- se asocia a un departamento con perfil de zona similar
-- (columna region_trends_patron de gold_dim_distrito_v1).
--
-- PASO PREVIO 1: asignar los patrones de region de Colombia
-- =============================================================
UPDATE lumina_gold.gold_dim_distrito_v1
SET region_trends_patron = CASE distrito_id
  WHEN 1 THEN 'bogot'            -- Distrito Central  (COMERCIAL)   -> Bogota
  WHEN 2 THEN 'atl[aá]ntico'     -- Puerto Norte      (MIXTO)       -> Atlantico
  WHEN 3 THEN 'valle del cauca'  -- Valle Sur         (RESIDENCIAL) -> Valle del Cauca
  WHEN 4 THEN 'antioquia'        -- Las Colinas       (RESIDENCIAL) -> Antioquia
  WHEN 5 THEN 'bol[ií]var'       -- Ribera Este       (MIXTO)       -> Bolivar
  WHEN 6 THEN '^santander'       -- Nuevo Horizonte   (COMERCIAL)   -> Santander Department
  WHEN 7 THEN 'cundinamarca'     -- San Data          (RESIDENCIAL) -> Cundinamarca
  WHEN 8 THEN 'risaralda'        -- Altos del Oeste   (MIXTO)       -> Risaralda
END
WHERE TRUE;

-- =============================================================
-- Tabla de contexto semanal por distrito
-- Filtrar SIEMPRE por refresh_date con fecha fija: la tabla esta
-- particionada por esa columna (una subconsulta MAX() escanea GBs).
-- =============================================================
CREATE OR REPLACE TABLE lumina_gold.gold_contexto_trends_distrito_v1 AS
WITH t AS (
  SELECT week, region_name, term, score
  FROM `bigquery-public-data.google_trends.international_top_terms`
  WHERE refresh_date = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND country_code = 'CO'
    AND week BETWEEN DATE '2026-08-23' AND DATE '2026-10-10'
)
SELECT
  d.distrito_id,
  d.distrito_nombre,
  t.week AS semana,
  COUNT(DISTINCT t.term) AS n_terminos_tendencia,
  ROUND(AVG(t.score), 2) AS trends_score_promedio,
  COUNT(DISTINCT IF(REGEXP_CONTAINS(LOWER(t.term),
        r'(banco|bancolombia|davivienda|nequi|daviplata|pr[eé]stamo|cr[eé]dito|ahorro|cdt|pse|dian|d[oó]lar|cesant[ií]as|prima|subsidio)'),
        t.term, NULL)) AS n_terminos_financieros
FROM t
JOIN lumina_gold.gold_dim_distrito_v1 d
  ON REGEXP_CONTAINS(LOWER(t.region_name), d.region_trends_patron)
GROUP BY 1, 2, 3;

-- Comprobacion: deben aparecer los 8 distritos con ~7 semanas cada uno
SELECT distrito_id, distrito_nombre, COUNT(*) AS semanas
FROM lumina_gold.gold_contexto_trends_distrito_v1
GROUP BY 1, 2 ORDER BY 1;
