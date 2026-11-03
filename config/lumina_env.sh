# =============================================================
# Lumina Bank - Variables de entorno (Grupo 1 - DataBank Metropolitan)
# Uso:  source ~/lumina-bank-bigdata/config/lumina_env.sh
# Ajusta REGION/ZONE si en la Entrega 1 definieron otra region.
# =============================================================
export PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
export PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
export REGION=us-central1
export ZONE=us-central1-c
export BUCKET=${PROJECT_ID}-lumina-lake
export CLUSTER=lumina-spark
export SA=${PROJECT_NUMBER}-compute@developer.gserviceaccount.com
export TOPIC=lumina-transacciones
export SUBSCRIPTION=lumina-transacciones-df
export BT_INSTANCE=lumina-rt
export BT_TABLE=perfil_cliente

echo "Proyecto: $PROJECT_ID | Region: $REGION | Bucket: gs://$BUCKET"
