#!/usr/bin/env bash
# =============================================================
# Paso 3 - Cluster de Managed Service for Apache Spark (Dataproc)
#   Un nodo de 32 GB, sin IP publica y con borrado automatico tras 1 h inactivo.
#   Disco de 100 GB para no superar la cuota regional de 500 GB.
# Si n4 no esta disponible en la zona: MAQUINA=n2-highmem-4 DISCO=pd-balanced bash ...
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh
MAQUINA=${MAQUINA:-n4-highmem-4}
DISCO=${DISCO:-hyperdisk-balanced}

gcloud dataproc clusters create "$CLUSTER" --region="$REGION" --zone="$ZONE" \
  --single-node --master-machine-type="$MAQUINA" \
  --master-boot-disk-type="$DISCO" --master-boot-disk-size=100 \
  --image-version=2.3-debian12 --no-address \
  --service-account="$SA" --scopes=cloud-platform \
  --max-idle=1h --labels=proyecto=lumina-bank,componente=batch

gcloud dataproc clusters list --region="$REGION"
