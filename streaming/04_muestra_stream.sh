#!/usr/bin/env bash
# =============================================================
# Paso 8 - Muestra para el simulador: 2 000 fraudes + 18 000 legitimas de la
# tabla de hechos gold, en orden aleatorio (streaming/muestra_stream.csv)
# =============================================================
set -euo pipefail
source ~/lumina-bank-bigdata/config/lumina_env.sh
cd ~/lumina-bank-bigdata/streaming

bq -q query --use_legacy_sql=false --format=csv --max_rows=20000 '
SELECT tipo, monto, cliente_origen_id, cliente_destino_id,
       saldo_ini_origen, saldo_fin_origen, canal, es_fraude
FROM (
  (SELECT * FROM lumina_gold.gold_fact_transaccion_v1 WHERE es_fraude = 1
   ORDER BY FARM_FINGERPRINT(transaccion_id) LIMIT 2000)
  UNION ALL
  (SELECT * FROM lumina_gold.gold_fact_transaccion_v1 WHERE es_fraude = 0
   ORDER BY FARM_FINGERPRINT(transaccion_id) LIMIT 18000)
)
ORDER BY RAND()' > muestra_stream.csv

echo ">> Filas (incluye encabezado): $(wc -l < muestra_stream.csv)"
head -3 muestra_stream.csv
