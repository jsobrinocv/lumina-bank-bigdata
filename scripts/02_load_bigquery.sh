#!/usr/bin/env bash
# =============================================================
# Paso 5 - Publicar SILVER (tabla externa) y cargar GOLD en BigQuery
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh

echo ">> Silver como tabla externa (los datos se quedan en el Data Lake)..."
bq mkdef --source_format=PARQUET \
  --hive_partitioning_mode=AUTO \
  --hive_partitioning_source_uri_prefix="gs://${BUCKET}/silver/transacciones" \
  "gs://${BUCKET}/silver/transacciones/*" > /tmp/silver_def.json
bq rm -f -t lumina_silver.silver_transaccion_v1 || true
bq mk --table --external_table_definition=/tmp/silver_def.json lumina_silver.silver_transaccion_v1

echo ">> Gold: tabla de hechos particionada por fecha y clusterizada..."
bq load --replace --source_format=PARQUET \
  --time_partitioning_field=fecha --time_partitioning_type=DAY \
  --clustering_fields=distrito_id,tipo \
  lumina_gold.gold_fact_transaccion_v1 "gs://${BUCKET}/gold/fact_transaccion/*.parquet"

echo ">> Gold: dimensiones y patron de gasto..."
for T in dim_cliente dim_fecha dim_distrito dim_tipo_transaccion patron_gasto; do
  bq load --replace --source_format=PARQUET \
    "lumina_gold.gold_${T}_v1" "gs://${BUCKET}/gold/${T}/*.parquet"
  echo "   ok gold_${T}_v1"
done

bq ls lumina_gold
