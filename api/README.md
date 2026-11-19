# API de exposición – Lumina Bank (Entrega 3)

API en **Cloud Run** (FastAPI) que entrega los datos procesados por la plataforma a la App, los tableros y los modelos.

| Método | Ruta | Fuente | Para qué |
|---|---|---|---|
| GET | `/health` | – | Estado del servicio |
| POST | `/v1/transacciones` | Pub/Sub | La App envía una transacción; responde 202, el id y una decisión preliminar |
| GET | `/v1/transacciones/{id}` | BigQuery `lumina_rt.rt_transaccion_scored_v1` | Decisión de Dataflow: APROBAR / REVISAR / BLOQUEAR |
| GET | `/v1/clientes/{id}/riesgo` | Cache → Bigtable `perfil_cliente` | Perfil de riesgo en milisegundos |
| GET | `/v1/liquidez/{distrito_id}` | BigQuery `lumina_gold.gold_plan_liquidez_v1` | Efectivo a provisionar a 7 días |
| GET | `/v1/kpis/{fecha}` | BigQuery `gold_kpi_operativo_diario_v1` | KPIs e ingresos por producto |
| GET | `/v1/operacion/ultimos-minutos` | BigQuery `lumina_rt.rt_agg_minuto_v1` | Transacciones y alertas por minuto |
| GET | `/v1/alertas?limite=&decision=` | BigQuery `lumina_rt.rt_transaccion_scored_v1` | Últimas transacciones REVISAR o BLOQUEAR |
| GET | `/v1/kpis?fecha=&ultimos=` | BigQuery `gold_kpi_operativo_diario_v1` | KPIs de una fecha o de los últimos N días |
| GET | `/v1/distritos` | BigQuery `gold_dim_distrito_v1` | Catálogo de distritos con tipo de zona |

Alias para el agente conversacional (`agente/`): `/salud` = `/health` y `/v1/clientes/{id}/perfil` = `/v1/clientes/{id}/riesgo`.

Documentación interactiva: `<URL>/docs`.

## Diseño

- **Mismo contrato que el simulador:** `POST /v1/transacciones` publica en `lumina-transacciones` con el esquema de `streaming/producer.py`, así el pipeline de Dataflow de la Entrega 2 no cambia.
- **Cache:** Memorystore en producción (`REDIS_HOST`); sin esa variable usa una cache en memoria del contenedor, sin costo, para el prototipo. TTL de 30 s para perfiles y 1 h para analítica.
- **Costo controlado:** cada consulta a BigQuery es parametrizada, filtra por partición y tiene un tope de 1 GB facturable.
- **Seguridad:** cuenta de servicio dedicada (lectura de Bigtable y BigQuery, publicación solo en el tópico), API key en Secret Manager, Cloud Run sin acceso anónimo; en producción, entrada única por Load Balancer + Cloud Armor (`deploy_lb_armor.sh`).
- **Consistencia:** `app/reglas.py` usa la misma fórmula hash que Spark y Dataflow para distrito y segmento.

## Uso

```bash
bash api/deploy.sh                                   # despliegue en Cloud Run
python3 api/demo/demo_e2e.py --url <URL>             # demo end-to-end en vivo
cd api && python3 -m pytest -q tests                 # pruebas de lógica
```

## Agente conversacional (ADK) sobre esta API

Cloud Run exige un token de Google además de la API key; el túnel de `gcloud` lo agrega:

```bash
gcloud run services proxy lumina-api --region $REGION --port 8081 &
# en agente/lumina_agente/.env:
#   LUMINA_API_URL=http://localhost:8081
#   LUMINA_API_KEY=<valor del secreto lumina-api-key>
```
