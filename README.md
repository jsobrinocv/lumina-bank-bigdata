# Lumina Bank – Plataforma de datos (Grupo 1 · DataBank Metropolitan)

Taller de Arquitectura Big Data – UTEC. Entrega 2: procesamiento, transformación y modelado.

## Arquitectura (resumen)

- **Batch (Spark en Dataproc):** `bronze` (CSV PaySim en GCS) → `silver` (Parquet limpio, particionado por fecha) → `gold` (modelo estrella cargado en BigQuery).
- **Tiempo real (Dataflow):** Pub/Sub → enriquecimiento + score de riesgo → BigQuery (`lumina_rt`) y Bigtable (perfil del cliente).
- **Analítica (BigQuery SQL / BigQuery ML):** cruce con SEC (benchmark bancario) y Google Trends (demanda digital); pronóstico de liquidez (ARIMA_PLUS) y modelo de fraude (regresión logística).

## Estructura

```
config/      variables de entorno (lumina_env.sh)
data/        eventos de calendario (supuestos de negocio)
spark/       jobs PySpark bronze->silver y silver->gold
sql/         modelado y analítica en BigQuery
streaming/   productor Pub/Sub y pipeline Apache Beam (Dataflow)
scripts/     setup, carga a BigQuery, orquestación batch y apagado
contracts/   contrato de datos de la capa silver
api/         capa de exposición (Entrega 3)
docs/        documentos de cada entrega, diagramas y evidencias
```

Los archivos `sql/consola_*.sql` son la versión para ejecutar desde BigQuery Studio
(equivalen a los pasos de terminal `02_load_bigquery.sh` y `03_setup_streaming.sh`).

## Entregas

| Entrega | Fecha | Contenido en el repo |
|---|---|---|
| 1 – Blueprint | 24-oct-2026 | `docs/entrega1/` |
| 2 – Procesamiento y modelado | 7-nov-2026 | `spark/`, `sql/`, `streaming/`, `docs/evidencias/entrega2/` |
| 3 – Exposición y demo | 20-nov-2026 | `api/`, `docs/entrega3/`, `docs/evidencias/entrega3/` |

## Ejecución rápida

```bash
source config/lumina_env.sh
bash scripts/00_setup.sh          # una vez
bash scripts/run_batch.sh         # flujo batch completo
bash streaming/03_setup_streaming.sh
# lanzar pipeline_streaming.py en Dataflow y luego producer.py (ver guía)
bash scripts/99_teardown.sh       # al terminar
```

## Convención de nombres

`<dataset>.<capa>_<entidad>_v<major>`, p. ej. `lumina_gold.gold_fact_transaccion_v1`.
