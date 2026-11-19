"""
Lumina Bank - API de exposicion de datos (Entrega 3)

Expone los datos procesados por la plataforma hacia las aplicaciones consumidoras
(app movil, cajeros, agentes, tableros y el agente conversacional ADK).

Endpoints principales (prefijo /v1):
  POST /transacciones                 publica una transaccion en Pub/Sub -> Dataflow la califica
  GET  /clientes/{cliente_id}/perfil  ultimo estado del cliente en Bigtable (milisegundos)
  GET  /alertas                       ultimas transacciones REVISAR / BLOQUEAR (BigQuery lumina_rt)
  GET  /liquidez/{distrito_id}        plan de efectivo a 7 dias (BigQuery ML, lumina_gold)
  GET  /kpis                          KPIs operativos diarios (lumina_gold)
  GET  /distritos                     catalogo de distritos

Documentacion interactiva: /docs (Swagger).  Salud del servicio: /salud (sin clave).
"""
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import date, datetime, timezone
from typing import Literal, Optional

import google.auth
from fastapi import FastAPI, HTTPException, Query, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("lumina-api")

# --------------------------------------------------------------------------
# Configuracion (variables de entorno de Cloud Run)
# --------------------------------------------------------------------------
PROJECT = os.environ.get("PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
if not PROJECT:
    try:
        _, PROJECT = google.auth.default()
    except Exception:  # sin credenciales (prueba local sin GCP)
        PROJECT = None
BT_INSTANCE = os.environ.get("BT_INSTANCE", "lumina-rt--lumina-rt")
BT_TABLE = os.environ.get("BT_TABLE", "perfil_cliente")
TOPIC = os.environ.get("TOPIC", "lumina-transacciones")
DS_GOLD = os.environ.get("DATASET_GOLD", "lumina_gold")
DS_RT = os.environ.get("DATASET_RT", "lumina_rt")
API_KEY = os.environ.get("API_KEY", "")          # vacio = sin clave (solo para pruebas locales)
CACHE_TTL = int(os.environ.get("CACHE_TTL_SEG", "300"))
RIESGOSOS = ("TRANSFER", "CASH_OUT")

app = FastAPI(
    title="Lumina Bank - API de datos",
    version="3.0.0",
    description="Capa de exposicion de la plataforma de datos de Lumina Bank (Grupo 1 - DataBank Metropolitan). "
                "Usa el boton **Authorize** con la clave `X-API-Key` para probar los endpoints.",
)

# --------------------------------------------------------------------------
# Seguridad: clave en el encabezado X-API-Key (visible en Swagger como Authorize)
# --------------------------------------------------------------------------
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verificar_clave(clave: Optional[str] = Security(_api_key_header)):
    if API_KEY and clave != API_KEY:
        raise HTTPException(status_code=401, detail="Clave X-API-Key invalida o ausente")


# --------------------------------------------------------------------------
# Clientes de Google Cloud (se crean una sola vez, al primer uso)
# --------------------------------------------------------------------------
_clientes = {}


def bq():
    if "bq" not in _clientes:
        from google.cloud import bigquery
        _clientes["bq"] = bigquery.Client(project=PROJECT)
    return _clientes["bq"]


def tabla_bt():
    if "bt" not in _clientes:
        from google.cloud import bigtable
        _clientes["bt"] = bigtable.Client(project=PROJECT).instance(BT_INSTANCE).table(BT_TABLE)
    return _clientes["bt"]


def publisher():
    if "ps" not in _clientes:
        from google.cloud import pubsub_v1
        _clientes["ps"] = pubsub_v1.PublisherClient()
    return _clientes["ps"]


# --------------------------------------------------------------------------
# Cache en memoria con expiracion (en produccion: Memorystore for Redis)
# --------------------------------------------------------------------------
_cache = {}


def cache_get(clave):
    item = _cache.get(clave)
    if item and time.time() - item[0] < CACHE_TTL:
        return item[1]
    return None


def cache_set(clave, valor):
    _cache[clave] = (time.time(), valor)
    return valor


def consulta(sql, params=None, cache_clave=None):
    """Ejecuta una consulta parametrizada en BigQuery y devuelve una lista de dicts."""
    if cache_clave:
        en_cache = cache_get(cache_clave)
        if en_cache is not None:
            return en_cache
    from google.cloud import bigquery
    config = bigquery.QueryJobConfig(query_parameters=params or [])
    filas = [dict(r.items()) for r in bq().query(sql, job_config=config).result()]
    for f in filas:  # fechas a texto para JSON
        for k, v in f.items():
            if isinstance(v, (date, datetime)):
                f[k] = v.isoformat()
    return cache_set(cache_clave, filas) if cache_clave else filas


# --------------------------------------------------------------------------
# Reglas de negocio compartidas con el pipeline de Dataflow (misma formula)
# --------------------------------------------------------------------------
def _h(texto, inicio, largo=8):
    return int(hashlib.md5(texto.encode("utf-8")).hexdigest()[inicio - 1:inicio - 1 + largo], 16)


def distrito_de(cliente_id):
    return _h(cliente_id, 1) % 8 + 1


def segmento_de(cliente_id):
    u = _h(cliente_id, 9) % 100
    return "18-25" if u < 20 else "26-40" if u < 55 else "41-60" if u < 85 else "60+"


def patron_gasto():
    sql = f"SELECT tipo, segmento_etario, monto_p95 FROM `{PROJECT}.{DS_GOLD}.gold_patron_gasto_v1`"
    filas = consulta(sql, cache_clave="patron_gasto")
    return {(f["tipo"], f["segmento_etario"]): f["monto_p95"] for f in filas}


def calificar(tipo, monto, cliente_id, saldo_ini, saldo_fin):
    """Score 0-100 y decision, igual que EnriquecerYCalificar del pipeline streaming."""
    p95 = patron_gasto().get((tipo, segmento_de(cliente_id)))
    score, motivos = 0, []
    if tipo in RIESGOSOS:
        score += 30; motivos.append("tipo_riesgoso")
        if saldo_ini > 0 and monto >= saldo_ini:
            score += 35; motivos.append("vacia_cuenta")
        if abs(saldo_ini - monto - saldo_fin) > 1:
            score += 15; motivos.append("descuadre_saldo")
    if p95 and monto > p95:
        score += 20; motivos.append("monto_sobre_p95_historico")
    score = min(score, 100)
    decision = "BLOQUEAR" if score >= 80 else "REVISAR" if score >= 50 else "APROBAR"
    return score, decision, motivos


# --------------------------------------------------------------------------
# Modelos de entrada
# --------------------------------------------------------------------------
class TransaccionIn(BaseModel):
    tipo: Literal["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
    monto: float = Field(gt=0, description="Monto de la operacion")
    cliente_origen_id: str = Field(pattern=r"^C[0-9]{6,10}$", examples=["C1231006815"])
    cliente_destino_id: str = Field(default="C0000000000", examples=["C1666544295"])
    saldo_ini_origen: float = Field(ge=0, examples=[181.0])
    saldo_fin_origen: float = Field(ge=0, examples=[0.0])
    canal: Literal["APP", "CAJERO", "AGENTE", "OFICINA"] = "APP"


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
def inicio():
    return {"servicio": "Lumina Bank - API de datos", "version": app.version,
            "documentacion": "/docs", "salud": "/salud"}


@app.get("/salud", tags=["operacion"])
def salud():
    """Prueba de vida para Cloud Run y el monitoreo (no requiere clave)."""
    return {"estado": "ok", "proyecto": PROJECT, "hora_utc": datetime.now(timezone.utc).isoformat()}


@app.post("/v1/transacciones", tags=["tiempo real"], status_code=202, dependencies=[Security(verificar_clave)])
def registrar_transaccion(tx: TransaccionIn):
    """Publica la transaccion en Pub/Sub. Dataflow la califica y actualiza BigQuery y Bigtable en segundos.

    Devuelve ademas una **decision preliminar** calculada con las mismas reglas del pipeline,
    para que el canal pueda responder al cliente sin esperar.
    """
    ahora = datetime.now(timezone.utc)
    msg = {
        "transaccion_id": str(uuid.uuid4()),
        "ts_evento": ahora.isoformat(),
        **tx.model_dump(),
        "etiqueta_fraude": 0,
    }
    try:
        ruta = publisher().topic_path(PROJECT, TOPIC)
        message_id = publisher().publish(ruta, json.dumps(msg).encode("utf-8"), origen="api").result(timeout=10)
    except Exception as e:
        log.exception("Error publicando en Pub/Sub")
        raise HTTPException(status_code=503, detail=f"No se pudo publicar en Pub/Sub: {e}")
    score, decision, motivos = calificar(tx.tipo, tx.monto, tx.cliente_origen_id,
                                         tx.saldo_ini_origen, tx.saldo_fin_origen)
    return {
        "transaccion_id": msg["transaccion_id"],
        "pubsub_message_id": message_id,
        "estado": "publicada",
        "decision_preliminar": decision,
        "score_riesgo": score,
        "motivos": motivos,
        "distrito_id": distrito_de(tx.cliente_origen_id),
        "segmento_etario": segmento_de(tx.cliente_origen_id),
        "siguiente_paso": f"/v1/clientes/{tx.cliente_origen_id}/perfil (en ~5-10 s, tras el paso por Dataflow)",
    }


@app.get("/v1/clientes/{cliente_id}/perfil", tags=["tiempo real"], dependencies=[Security(verificar_clave)])
def perfil_cliente(cliente_id: str):
    """Ultimo estado del cliente escrito por Dataflow en Bigtable (lectura por row key en milisegundos)."""
    inicio = time.perf_counter()
    try:
        fila = tabla_bt().read_row(cliente_id.encode("utf-8"))
    except Exception as e:
        log.exception("Error leyendo Bigtable")
        raise HTTPException(status_code=503, detail=f"No se pudo leer Bigtable: {e}")
    if fila is None:
        raise HTTPException(status_code=404, detail=f"El cliente {cliente_id} no tiene transacciones en tiempo real")
    perfil = {}
    for columna, celdas in fila.cells.get("perfil", {}).items():
        perfil[columna.decode().replace("ultimo_", "")] = celdas[0].value.decode()
    return {
        "cliente_id": cliente_id,
        "segmento_etario": segmento_de(cliente_id),
        "ultima_transaccion": perfil,
        "latencia_lectura_ms": round((time.perf_counter() - inicio) * 1000, 1),
        "fuente": f"Bigtable {BT_INSTANCE}/{BT_TABLE}",
    }


@app.get("/v1/alertas", tags=["tiempo real"], dependencies=[Security(verificar_clave)])
def alertas_recientes(limite: int = Query(20, ge=1, le=200),
                      decision: Optional[Literal["REVISAR", "BLOQUEAR"]] = None):
    """Ultimas transacciones que el motor envio a REVISAR o BLOQUEAR (BigQuery, ultimas 24 h)."""
    from google.cloud import bigquery
    filtro = "AND decision = @decision" if decision else "AND decision != 'APROBAR'"
    sql = f"""
        SELECT transaccion_id, ts_proceso, tipo, monto, cliente_origen_id, distrito_id,
               canal, score_riesgo, decision, motivos, latencia_ms
        FROM `{PROJECT}.{DS_RT}.rt_transaccion_scored_v1`
        WHERE ts_proceso >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) {filtro}
        ORDER BY ts_proceso DESC
        LIMIT @limite"""
    params = [bigquery.ScalarQueryParameter("limite", "INT64", limite)]
    if decision:
        params.append(bigquery.ScalarQueryParameter("decision", "STRING", decision))
    filas = consulta(sql, params)
    return {"total": len(filas), "alertas": filas}


@app.get("/v1/liquidez/{distrito_id}", tags=["batch"], dependencies=[Security(verificar_clave)])
def plan_liquidez(distrito_id: int):
    """Plan de efectivo a 7 dias del distrito (pronostico ARIMA_PLUS de BigQuery ML + 10 % de colchon)."""
    if not 1 <= distrito_id <= 8:
        raise HTTPException(status_code=404, detail="El distrito_id debe estar entre 1 y 8")
    from google.cloud import bigquery
    sql = f"""
        SELECT distrito_id, distrito_nombre, fecha, retiro_esperado,
               retiro_escenario_alto, efectivo_a_provisionar
        FROM `{PROJECT}.{DS_GOLD}.gold_plan_liquidez_v1`
        WHERE distrito_id = @d ORDER BY fecha"""
    filas = consulta(sql, [bigquery.ScalarQueryParameter("d", "INT64", distrito_id)],
                     cache_clave=f"liquidez_{distrito_id}")
    total = round(sum(f["efectivo_a_provisionar"] for f in filas), 2)
    return {"distrito_id": distrito_id,
            "distrito_nombre": filas[0]["distrito_nombre"] if filas else None,
            "efectivo_total_7_dias": total, "plan": filas}


@app.get("/v1/kpis", tags=["batch"], dependencies=[Security(verificar_clave)])
def kpis_operativos(fecha: Optional[date] = Query(None, description="AAAA-MM-DD; si se omite, ultimos dias"),
                    ultimos: int = Query(7, ge=1, le=60)):
    """KPIs operativos diarios: volumen, ingresos estimados, costo por transaccion y fraude."""
    from google.cloud import bigquery
    base = f"SELECT * FROM `{PROJECT}.{DS_GOLD}.gold_kpi_operativo_diario_v1`"
    if fecha:
        filas = consulta(base + " WHERE fecha = @f", [bigquery.ScalarQueryParameter("f", "DATE", fecha)],
                         cache_clave=f"kpi_{fecha}")
        if not filas:
            raise HTTPException(status_code=404, detail=f"No hay KPIs para {fecha}")
        return filas[0]
    filas = consulta(base + " ORDER BY fecha DESC LIMIT @n", [bigquery.ScalarQueryParameter("n", "INT64", ultimos)],
                     cache_clave=f"kpi_ultimos_{ultimos}")
    return {"dias": len(filas), "kpis": filas}


@app.get("/v1/distritos", tags=["batch"], dependencies=[Security(verificar_clave)])
def distritos():
    """Catalogo de distritos (dimension de la capa gold)."""
    sql = f"SELECT * FROM `{PROJECT}.{DS_GOLD}.gold_dim_distrito_v1` ORDER BY distrito_id"
    return consulta(sql, cache_clave="distritos")
