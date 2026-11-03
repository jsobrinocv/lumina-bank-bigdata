#!/usr/bin/env bash
# =============================================================
# Paso 1 - Preparacion del entorno (ejecutar UNA vez en Cloud Shell)
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh

echo ">> 1. Habilitando APIs..."
gcloud services enable \
  dataproc.googleapis.com dataflow.googleapis.com pubsub.googleapis.com \
  bigquery.googleapis.com bigtable.googleapis.com bigtableadmin.googleapis.com \
  storage.googleapis.com compute.googleapis.com

echo ">> 2. Private Google Access en la subred default (necesario para --no-address)..."
gcloud compute networks subnets update default \
  --region="$REGION" --enable-private-ip-google-access

echo ">> 3. Roles para la cuenta de servicio de Compute ($SA)..."
for ROLE in roles/dataproc.worker roles/dataflow.worker roles/storage.objectAdmin \
            roles/bigquery.dataEditor roles/bigquery.jobUser roles/pubsub.subscriber \
            roles/pubsub.viewer roles/bigtable.user; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA}" --role="$ROLE" --condition=None --quiet > /dev/null
  echo "   ok $ROLE"
done

echo ">> 4. Bucket del Data Lake (medallion: bronze / silver / gold)..."
gcloud storage buckets create "gs://${BUCKET}" --location="$REGION" \
  --uniform-bucket-level-access || echo "   (el bucket ya existe)"

echo ">> 5. Datasets de BigQuery (ubicacion US para poder cruzar con datasets publicos)..."
for DS in lumina_silver lumina_gold lumina_rt; do
  bq --location=US mk -d --description "Lumina Bank - capa ${DS#lumina_}" \
     "${PROJECT_ID}:${DS}" || echo "   (dataset $DS ya existe)"
done

echo ">> Listo."
