#!/usr/bin/env bash
# =============================================================
# Orquestacion simple del flujo batch completo (re-ejecutable):
#   Spark bronze->silver -> Spark silver->gold -> carga BigQuery -> SQL gold
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh
cd ~/lumina-bank-bigdata

echo "== Subiendo scripts al bucket"
gcloud storage cp spark/*.py "gs://${BUCKET}/scripts/"

echo "== Job 1: bronze -> silver"
gcloud dataproc jobs submit pyspark "gs://${BUCKET}/scripts/01_bronze_to_silver.py" \
  --cluster="$CLUSTER" --region="$REGION" -- --bucket="$BUCKET"

echo "== Job 2: silver -> gold"
gcloud dataproc jobs submit pyspark "gs://${BUCKET}/scripts/02_silver_to_gold.py" \
  --cluster="$CLUSTER" --region="$REGION" -- --bucket="$BUCKET"

echo "== Carga en BigQuery"
bash scripts/02_load_bigquery.sh

for f in sql/00_validacion_gold.sql sql/01_benchmark_sec.sql sql/02_contexto_trends.sql \
         sql/03_liquidez.sql sql/04_kpis_operativos.sql sql/05_modelo_fraude.sql; do
  echo "== SQL: $f"
  bq query --use_legacy_sql=false < "$f"
done
echo "== Flujo batch terminado"
