#!/usr/bin/env bash
# =============================================================
# Apagar lo que cobra por hora (ejecutar al terminar cada sesion de trabajo)
# Los datos en GCS y BigQuery se conservan para la Entrega 3.
# =============================================================
source ~/lumina-bank-bigdata/config/lumina_env.sh

echo ">> Drenando jobs de Dataflow activos..."
for JOB in $(gcloud dataflow jobs list --region="$REGION" --status=active --format="value(JOB_ID)"); do
  gcloud dataflow jobs drain "$JOB" --region="$REGION"
done

echo ">> Borrando cluster Dataproc..."
gcloud dataproc clusters delete "$CLUSTER" --region="$REGION" --quiet || true

echo ">> Borrando instancia Bigtable (si existe)..."
gcloud bigtable instances delete "$BT_INSTANCE" --quiet || true

echo ">> Listo. Revisa Facturacion > Informes en la consola."
