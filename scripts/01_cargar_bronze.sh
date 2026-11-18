#!/usr/bin/env bash
# =============================================================
# Paso 2 - Cargar los datos crudos a la capa bronze del Data Lake
#   PaySim (Kaggle: ealaxi/paysim1, 6 362 620 filas) + calendario de eventos
# Requisito: token de Kaggle en ~/.kaggle/kaggle.json (Kaggle > Settings > API > Create New Token)
# Alternativa sin Kaggle API: descargar el CSV en tu PC y subirlo desde la consola
#   a gs://<BUCKET>/bronze/paysim/ (Cloud Storage > bucket > Subir archivos)
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh

if ! gcloud storage ls "gs://${BUCKET}/bronze/paysim/*.csv" > /dev/null 2>&1; then
  echo ">> Descargando PaySim desde Kaggle..."
  pip install --user --quiet kaggle
  chmod 600 ~/.kaggle/kaggle.json
  mkdir -p ~/paysim
  ~/.local/bin/kaggle datasets download -d ealaxi/paysim1 -p ~/paysim --unzip
  echo ">> Subiendo a bronze (copia inmutable de la fuente)..."
  gcloud storage cp ~/paysim/*.csv "gs://${BUCKET}/bronze/paysim/"
  rm -rf ~/paysim   # libera espacio en Cloud Shell (5 GB)
else
  echo "   (PaySim ya esta en bronze)"
fi

echo ">> Calendario de eventos de DataLandia..."
gcloud storage cp ~/lumina-bank-bigdata/data/eventos_calendario.csv "gs://${BUCKET}/bronze/eventos/"

gcloud storage ls -l "gs://${BUCKET}/bronze/**"
