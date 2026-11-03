-- =============================================================
-- Version CONSOLA del Paso 9.1: tablas de tiempo real en BigQuery
-- (equivale a la parte "bq mk" de streaming/03_setup_streaming.sh)
-- =============================================================

CREATE TABLE IF NOT EXISTS lumina_rt.rt_transaccion_scored_v1 (
  transaccion_id     STRING,
  ts_evento          TIMESTAMP,
  ts_proceso         TIMESTAMP,
  latencia_ms        INT64,
  tipo               STRING,
  monto              FLOAT64,
  cliente_origen_id  STRING,
  cliente_destino_id STRING,
  distrito_id        INT64,
  segmento_etario    STRING,
  canal              STRING,
  score_riesgo       INT64,
  decision           STRING,
  motivos            STRING,
  etiqueta_fraude    INT64
)
PARTITION BY DATE(ts_evento)
CLUSTER BY distrito_id, decision;

CREATE TABLE IF NOT EXISTS lumina_rt.rt_agg_minuto_v1 (
  ventana_inicio TIMESTAMP,
  ventana_fin    TIMESTAMP,
  distrito_id    INT64,
  tipo           STRING,
  n_tx           INT64,
  monto_total    FLOAT64,
  n_alertas      INT64
)
PARTITION BY DATE(ventana_inicio);
