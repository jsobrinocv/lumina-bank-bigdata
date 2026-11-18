# API de exposición de datos – Lumina Bank (Entrega 3)

Microservicio **FastAPI** desplegado en **Cloud Run** que expone los datos procesados por la
plataforma a las aplicaciones consumidoras (app móvil, cajeros, agentes, tableros y el agente ADK).

| Método | Ruta | Fuente | Para qué |
|---|---|---|---|
| `POST` | `/v1/transacciones` | Pub/Sub → Dataflow | Registra una transacción; devuelve la decisión preliminar y Dataflow la califica |
| `GET` | `/v1/clientes/{cliente_id}/perfil` | Bigtable | Último estado del cliente, lectura en milisegundos |
| `GET` | `/v1/alertas?limite=20&decision=BLOQUEAR` | BigQuery `lumina_rt` | Últimas alertas REVISAR/BLOQUEAR (24 h) |
| `GET` | `/v1/liquidez/{distrito_id}` | BigQuery ML `lumina_gold` | Plan de efectivo a 7 días por distrito |
| `GET` | `/v1/kpis?fecha=AAAA-MM-DD` | BigQuery `lumina_gold` | KPIs operativos diarios |
| `GET` | `/v1/distritos` | BigQuery `lumina_gold` | Catálogo de distritos |
| `GET` | `/salud` | — | Prueba de vida (sin clave) |

Documentación interactiva: `https://<url-del-servicio>/docs` → botón **Authorize** → clave `X-API-Key`.

## Seguridad

- Clave `X-API-Key` guardada en **Secret Manager** (`lumina-api-key`) e inyectada como variable `API_KEY`.
- Cuenta de servicio dedicada `lumina-api-sa` con mínimo privilegio:
  BigQuery Job User (proyecto), BigQuery Data Viewer (datasets `lumina_gold` y `lumina_rt`),
  Bigtable Reader (instancia), Pub/Sub Publisher (tema `lumina-transacciones`),
  Secret Manager Secret Accessor (secreto `lumina-api-key`).
- Consultas a BigQuery **parametrizadas** (sin concatenar entradas del usuario).
- Validación de entrada con Pydantic (tipos, rangos y formato de `cliente_id`).

## Despliegue (Cloud Shell)

```bash
source ~/lumina-bank-bigdata/config/lumina_env.sh
cd ~/lumina-bank-bigdata/api
gcloud run deploy lumina-api --source . --region $REGION \
  --service-account lumina-api-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --set-env-vars PROJECT_ID=$PROJECT_ID,BT_INSTANCE=$BT_INSTANCE,BT_TABLE=$BT_TABLE,TOPIC=$TOPIC \
  --set-secrets API_KEY=lumina-api-key:latest \
  --allow-unauthenticated --min-instances 0 --max-instances 3 \
  --memory 512Mi --cpu 1 --labels proyecto=lumina-bank,componente=api
```

## Pruebas locales (sin conectarse a GCP)

```bash
pip install -r requirements.txt pytest httpx
pytest -q test_api.py
```

## Decisiones de diseño

- **Cache en memoria** (5 min) para catálogos, patrón de gasto y planes de liquidez. En producción se
  reemplaza por **Memorystore for Redis** (incluido en la arquitectura y en la estimación de costos),
  compartido por todas las instancias.
- **Decisión preliminar en la API + decisión definitiva en Dataflow**: el canal responde al cliente al
  instante con las mismas reglas del pipeline, y el registro oficial queda en BigQuery y Bigtable.
- `min-instances 0` en el prototipo para no pagar en reposo; en producción `min-instances 1` evita el
  arranque en frío.
