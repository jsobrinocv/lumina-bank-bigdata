-- =============================================================
-- Vertical I - Modelo de fraude entrenado en batch (BigQuery ML)
-- En PaySim el fraude solo ocurre en TRANSFER y CASH_OUT.
-- Se usa todo el fraude + 20% de no-fraude (submuestreo) para bajar costo
-- y desbalance; auto_class_weights compensa el resto.
-- =============================================================

CREATE OR REPLACE TABLE lumina_gold.ml_dataset_fraude_v1 AS
SELECT
  f.tipo, f.monto, f.hora, f.canal, f.tipo_destino, c.segmento_etario,
  f.vacia_cuenta, f.saldo_ini_origen,
  f.error_saldo_origen, IFNULL(f.error_saldo_destino, 0) AS error_saldo_destino,
  f.es_fraude,
  IF(MOD(ABS(FARM_FINGERPRINT(f.transaccion_id)), 10) < 8, 'TRAIN', 'TEST') AS split
FROM lumina_gold.gold_fact_transaccion_v1 f
JOIN lumina_gold.gold_dim_cliente_v1 c ON f.cliente_origen_id = c.cliente_id
WHERE f.tipo IN ('TRANSFER', 'CASH_OUT')
  AND (f.es_fraude = 1 OR MOD(ABS(FARM_FINGERPRINT(CONCAT(f.transaccion_id, 's'))), 100) < 20);

CREATE OR REPLACE MODEL lumina_gold.ml_fraude_logreg_v1
OPTIONS (
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['es_fraude'],
  auto_class_weights = TRUE,
  data_split_method = 'NO_SPLIT'
) AS
SELECT * EXCEPT (split) FROM lumina_gold.ml_dataset_fraude_v1 WHERE split = 'TRAIN';

-- Metricas sobre el conjunto de prueba (precision, recall, f1, roc_auc)
SELECT * FROM ML.EVALUATE(
  MODEL lumina_gold.ml_fraude_logreg_v1,
  (SELECT * EXCEPT (split) FROM lumina_gold.ml_dataset_fraude_v1 WHERE split = 'TEST'));

-- Variables mas influyentes (para explicar el modelo en la sustentacion)
SELECT * FROM ML.WEIGHTS(MODEL lumina_gold.ml_fraude_logreg_v1)
ORDER BY ABS(weight) DESC;
