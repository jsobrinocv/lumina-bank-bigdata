-- =============================================================
-- Version CONSOLA del Paso 6 (equivale a scripts/02_load_bigquery.sh)
-- Ejecutar en BigQuery Studio DESPUES de los dos jobs de Spark.
--
-- ANTES DE EJECUTAR: usa Buscar y reemplazar (Ctrl+H en el editor de
-- BigQuery) y cambia  TU-PROYECTO  por el ID de tu proyecto GCP.
-- El bucket debe llamarse  <ID-del-proyecto>-lumina-lake
-- =============================================================

-- 1) SILVER como tabla externa (los datos se quedan en Cloud Storage)
CREATE OR REPLACE EXTERNAL TABLE lumina_silver.silver_transaccion_v1
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://TU-PROYECTO-lumina-lake/silver/transacciones/*'],
  hive_partition_uri_prefix = 'gs://TU-PROYECTO-lumina-lake/silver/transacciones'
);

-- 2) GOLD: tabla de hechos particionada por fecha y clusterizada
LOAD DATA OVERWRITE lumina_gold.gold_fact_transaccion_v1
PARTITION BY fecha
CLUSTER BY distrito_id, tipo
FROM FILES (format = 'PARQUET',
            uris = ['gs://TU-PROYECTO-lumina-lake/gold/fact_transaccion/*.parquet']);

-- 3) GOLD: dimensiones y patron de gasto
LOAD DATA OVERWRITE lumina_gold.gold_dim_cliente_v1
FROM FILES (format = 'PARQUET', uris = ['gs://TU-PROYECTO-lumina-lake/gold/dim_cliente/*.parquet']);

LOAD DATA OVERWRITE lumina_gold.gold_dim_fecha_v1
FROM FILES (format = 'PARQUET', uris = ['gs://TU-PROYECTO-lumina-lake/gold/dim_fecha/*.parquet']);

LOAD DATA OVERWRITE lumina_gold.gold_dim_distrito_v1
FROM FILES (format = 'PARQUET', uris = ['gs://TU-PROYECTO-lumina-lake/gold/dim_distrito/*.parquet']);

LOAD DATA OVERWRITE lumina_gold.gold_dim_tipo_transaccion_v1
FROM FILES (format = 'PARQUET', uris = ['gs://TU-PROYECTO-lumina-lake/gold/dim_tipo_transaccion/*.parquet']);

LOAD DATA OVERWRITE lumina_gold.gold_patron_gasto_v1
FROM FILES (format = 'PARQUET', uris = ['gs://TU-PROYECTO-lumina-lake/gold/patron_gasto/*.parquet']);

-- 4) Comprobacion rapida
SELECT table_id, row_count
FROM lumina_gold.__TABLES__
ORDER BY table_id;
