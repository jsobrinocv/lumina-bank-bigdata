#!/usr/bin/env bash
# =============================================================
# Paso 9 - Lanzar el pipeline de tiempo real en Dataflow
#   Workers sin IP publica, Streaming Engine y maximo 2 workers (control de costo).
#   El lanzador queda en segundo plano: el job sigue corriendo en Dataflow
#   aunque cierres Cloud Shell. Para detenerlo: bash scripts/99_teardown.sh
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh
cd ~/lumina-bank-bigdata/streaming

if [[ ! -d ~/venv-beam ]]; then
  python3 -m venv ~/venv-beam
  ~/venv-beam/bin/pip install --quiet -r requirements.txt
fi

BT_ARGS=()
if gcloud bigtable instances describe "$BT_INSTANCE" > /dev/null 2>&1; then
  BT_ARGS=(--bigtable_instance="$BT_INSTANCE" --bigtable_table="$BT_TABLE")
else
  echo "   (sin Bigtable: el perfil del cliente no se escribira)"
fi

nohup ~/venv-beam/bin/python pipeline_streaming.py \
  --runner=DataflowRunner --project="$PROJECT_ID" --region="$REGION" \
  --job_name=lumina-streaming \
  --temp_location="gs://${BUCKET}/tmp" --staging_location="gs://${BUCKET}/staging" \
  --service_account_email="$SA" \
  --no_use_public_ips --subnetwork="regions/${REGION}/subnetworks/default" \
  --enable_streaming_engine --machine_type=n1-standard-2 \
  --num_workers=1 --max_num_workers=2 \
  --input_subscription="projects/${PROJECT_ID}/subscriptions/${SUBSCRIPTION}" \
  --tabla_patron="${PROJECT_ID}.lumina_gold.gold_patron_gasto_v1" \
  --tabla_scored="${PROJECT_ID}:lumina_rt.rt_transaccion_scored_v1" \
  --tabla_agg="${PROJECT_ID}:lumina_rt.rt_agg_minuto_v1" \
  "${BT_ARGS[@]}" > ~/dataflow_lanzamiento.log 2>&1 &

echo ">> Lanzando... revisa el avance con: tail -f ~/dataflow_lanzamiento.log"
echo "   Espera a que el job aparezca como Running (3-5 min):"
echo "   gcloud dataflow jobs list --region=$REGION --status=active"
