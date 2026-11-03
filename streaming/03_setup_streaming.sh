#!/usr/bin/env bash
# =============================================================
# Paso 7 - Infraestructura de tiempo real: Pub/Sub, tablas BigQuery, Bigtable
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh

echo ">> Pub/Sub: topico + suscripcion para Dataflow..."
gcloud pubsub topics create "$TOPIC" || echo "   (topico ya existe)"
gcloud pubsub subscriptions create "$SUBSCRIPTION" --topic="$TOPIC" \
  --ack-deadline=60 --message-retention-duration=1d || echo "   (suscripcion ya existe)"

echo ">> BigQuery: tablas de tiempo real..."
bq mk --table \
  --time_partitioning_field=ts_evento --time_partitioning_type=DAY \
  --clustering_fields=distrito_id,decision \
  lumina_rt.rt_transaccion_scored_v1 \
  transaccion_id:STRING,ts_evento:TIMESTAMP,ts_proceso:TIMESTAMP,latencia_ms:INT64,tipo:STRING,monto:FLOAT64,cliente_origen_id:STRING,cliente_destino_id:STRING,distrito_id:INT64,segmento_etario:STRING,canal:STRING,score_riesgo:INT64,decision:STRING,motivos:STRING,etiqueta_fraude:INT64 \
  || echo "   (tabla scored ya existe)"

bq mk --table \
  --time_partitioning_field=ventana_inicio --time_partitioning_type=DAY \
  lumina_rt.rt_agg_minuto_v1 \
  ventana_inicio:TIMESTAMP,ventana_fin:TIMESTAMP,distrito_id:INT64,tipo:STRING,n_tx:INT64,monto_total:FLOAT64,n_alertas:INT64 \
  || echo "   (tabla agg ya existe)"

if [[ "${1:-}" == "--con-bigtable" ]]; then
  echo ">> Bigtable (OPCIONAL, cobra por hora: borrar al terminar la demo)..."
  gcloud bigtable instances create "$BT_INSTANCE" --display-name="Lumina RT" \
    --cluster-config=id=${BT_INSTANCE}-c1,zone=${ZONE},nodes=1 || echo "   (instancia ya existe)"
  gcloud bigtable instances tables create "$BT_TABLE" --instance="$BT_INSTANCE" \
    --column-families="perfil:maxversions=5" || echo "   (tabla ya existe)"
fi
echo ">> Listo."
