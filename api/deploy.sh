#!/usr/bin/env bash
# =============================================================
# Entrega 3 - Despliegue de la API de Lumina Bank en Cloud Run
# Uso (Cloud Shell, desde la raiz del repositorio):
#   bash api/deploy.sh                 # prototipo: cache en memoria del contenedor
#   REDIS_HOST=10.x.x.x bash api/deploy.sh   # produccion: con Memorystore
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh

SERVICE=lumina-api
API_SA=lumina-api-sa@${PROJECT_ID}.iam.gserviceaccount.com

echo ">> 1. APIs necesarias para construir y publicar el contenedor..."
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com secretmanager.googleapis.com

echo ">> 2. Cuenta de servicio dedicada (minimo privilegio, sin rol Editor)..."
gcloud iam service-accounts create lumina-api-sa \
  --display-name="Lumina API (Cloud Run)" || echo "   (ya existe)"
for ROLE in roles/bigtable.reader roles/bigquery.jobUser roles/bigquery.dataViewer; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${API_SA}" \
    --role="$ROLE" --condition=None --quiet > /dev/null
  echo "   ok $ROLE"
done
# Solo puede publicar en el topico de transacciones, no en todo el proyecto
gcloud pubsub topics add-iam-policy-binding "$TOPIC" \
  --member="serviceAccount:${API_SA}" --role=roles/pubsub.publisher > /dev/null
echo "   ok roles/pubsub.publisher sobre $TOPIC"

echo ">> 3. API key en Secret Manager (nunca en el codigo)..."
if ! gcloud secrets describe lumina-api-key > /dev/null 2>&1; then
  openssl rand -hex 24 | gcloud secrets create lumina-api-key --data-file=-
fi
gcloud secrets add-iam-policy-binding lumina-api-key \
  --member="serviceAccount:${API_SA}" --role=roles/secretmanager.secretAccessor > /dev/null

echo ">> 4. Construccion y despliegue en Cloud Run..."
RED=()
if [[ -n "${REDIS_HOST:-}" ]]; then
  # Direct VPC egress: Cloud Run llega a la IP privada de Memorystore sin conector adicional
  RED=(--network=default --subnet=default --vpc-egress=private-ranges-only)
fi
gcloud run deploy "$SERVICE" --source="$(dirname "$0")" --region="$REGION" \
  --service-account="$API_SA" --no-allow-unauthenticated \
  --cpu=1 --memory=512Mi --concurrency=80 --min-instances=1 --max-instances=10 \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},BT_INSTANCE=${BT_INSTANCE},BT_TABLE=${BT_TABLE},TOPIC=${TOPIC},REDIS_HOST=${REDIS_HOST:-}" \
  --set-secrets="API_KEY=lumina-api-key:latest" \
  --labels=proyecto=lumina-bank,componente=api "${RED[@]}"

URL=$(gcloud run services describe "$SERVICE" --region="$REGION" --format='value(status.url)')
echo ">> Listo: $URL"
echo "   Documentacion: $URL/docs (con token de identidad)"
echo "   Demo:  python3 api/demo/demo_e2e.py --url $URL"
