# Guion de la demo end-to-end (Entrega 3) – 8 minutos

Proceso de negocio demostrado: **decisión en tiempo real sobre una transacción** (vertical I), más la consulta del plan de liquidez y los KPIs (verticales II y III).

## Antes de la sustentación (30 min antes)

```bash
source ~/lumina-bank-bigdata/config/lumina_env.sh
bash streaming/03_setup_streaming.sh --con-bigtable      # Pub/Sub, tablas rt y Bigtable
# Lanzar el job de Dataflow de la Entrega 2 (pipeline_streaming.py) y esperar estado "Running"
bash api/deploy.sh                                        # API en Cloud Run (~3 min)
URL=$(gcloud run services describe lumina-api --region $REGION --format='value(status.url)')
python3 api/demo/demo_e2e.py --url $URL --repeticiones 10  # ensayo
```

Pestañas abiertas: grafo del job de Dataflow, suscripción de Pub/Sub, tabla `rt_transaccion_scored_v1` en BigQuery, tablero de Looker Studio y `<URL>/docs`.

## En vivo

| Min | Qué se muestra | Mensaje para el jurado |
|---|---|---|
| 0–1 | Diagrama de arquitectura final | El recorrido que vamos a ver: App → API → Pub/Sub → Dataflow → Bigtable/BigQuery → API |
| 1–2 | `<URL>/docs` | La capa de exposición: 6 endpoints documentados y protegidos |
| 2–5 | `demo_e2e.py` pasos 1 a 4 | La transacción normal se aprueba; la sospechosa se bloquea con sus motivos, en segundos |
| 5–6 | Grafo de Dataflow y tabla en BigQuery | Las mismas transacciones, procesadas por el pipeline real |
| 6–7 | `demo_e2e.py` pasos 5 a 7 | Latencia medida (p50/p95), plan de liquidez y KPIs servidos por la API |
| 7–8 | Simulador a 20 tx/s + tablero | La plataforma sostiene carga continua y alimenta el tablero operativo |

Para el paso de carga: `python3 streaming/producer.py --project $PROJECT_ID --topic $TOPIC --archivo muestra_stream.csv --tps 20 --max 1200`.

## Plan B

Si Dataflow no arranca a tiempo, mostrar el video de respaldo de la corrida del ensayo y consultar con la API las decisiones ya guardadas en BigQuery (`GET /v1/transacciones/{id}` del ensayo).

## Después de la sustentación

```bash
bash scripts/99_teardown.sh                               # borrar cluster, job y Bigtable
gcloud run services update lumina-api --region $REGION --min-instances 0
```
