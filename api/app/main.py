"""Lumina Bank - API de exposicion (Cloud Run).

Endpoints
  GET  /health                               estado del servicio
  POST /v1/transacciones                     recibe una transaccion y la publica en Pub/Sub (202)
  GET  /v1/transacciones/{id}                decision tomada por Dataflow (BigQuery lumina_rt)
  GET  /v1/clientes/{id}/riesgo              perfil de riesgo en milisegundos (cache -> Bigtable)
  GET  /v1/liquidez/{distrito_id}            plan de efectivo a 7 dias (BigQuery ML, capa gold)
  GET  /v1/kpis/{fecha}                      KPIs operativos e ingresos por producto del dia
  GET  /v1/operacion/ultimos-minutos         transacciones y alertas por minuto, distrito y tipo

Compatibles con el agente conversacional (agente/lumina_agente, ADK):
  GET  /salud                                igual que /health
  GET  /v1/clientes/{id}/perfil              igual que /v1/clientes/{id}/riesgo
  GET  /v1/alertas?limite=&decision=         ultimas transacciones REVISAR o BLOQUEAR
  GET  /v1/kpis?fecha=&ultimos=              KPIs de una fecha o de los ultimos N dias
  GET  /v1/distritos                         catalogo de distritos con su tipo de zona

Documentacion interactiva (OpenAPI): /docs
"""
import datetime as dt
import hmac
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Query, Request

from . import __version__, config, reglas
from .cache import crear_cache
from .modelos import (DecisionPreliminar, DecisionTransaccion, RiesgoCliente, TransaccionAceptada,
                      TransaccionIn)

log = logging.getLogger("lumina-api")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def crear_app(cfg=None, perfiles=None, analitica=None, publicador=None, cache=None) -> FastAPI:
    """Fabrica de la app. En produccion crea los clientes reales; en pruebas recibe dobles."""

    @asynccontextmanager
    async def ciclo_vida(app: FastAPI):
        c = cfg or config.cargar()
        app.state.cfg = c
        app.state.cache = cache or crear_cache(c.redis_host, c.redis_port)
        if perfiles is None or analitica is None or publicador is None:
            from .repositorios import AnaliticaRepo, PerfilRepo, Publicador
        app.state.perfiles = perfiles or PerfilRepo(c.project_id, c.bt_instance, c.bt_table)
        app.state.analitica = analitica or AnaliticaRepo(c.project_id, c.ds_gold, c.ds_rt,
                                                         c.max_bytes_consulta)
        app.state.publicador = publicador or Publicador(c.project_id, c.topic)
        yield

    app = FastAPI(title="Lumina Bank API", version=__version__, lifespan=ciclo_vida,
                  description="Capa de exposicion de la plataforma de datos de Lumina Bank "
                              "(Grupo 1 - DataBank Metropolitan, Entrega 3).")

    @app.middleware("http")
    async def medir_latencia(request: Request, call_next):
        inicio = time.perf_counter()
        respuesta = await call_next(request)
        ms = round((time.perf_counter() - inicio) * 1000, 1)
        respuesta.headers["X-Latencia-ms"] = str(ms)
        # Log estructurado: Cloud Logging lo indexa y Cloud Monitoring puede graficar la latencia
        log.info(json.dumps({"severity": "INFO", "ruta": request.url.path, "metodo": request.method,
                             "estado": respuesta.status_code, "latencia_ms": ms}))
        return respuesta

    def verificar_api_key(request: Request, x_api_key: str | None = Header(default=None)):
        esperado = request.app.state.cfg.api_key
        if esperado and not hmac.compare_digest(x_api_key or "", esperado):
            raise HTTPException(status_code=401, detail="API key invalida o ausente")

    protegido = [Depends(verificar_api_key)]

    @app.get("/health", tags=["operacion"])
    @app.get("/salud", tags=["operacion"], include_in_schema=False)
    def health(request: Request):
        return {"estado": "ok", "version": __version__,
                "cache": request.app.state.cache.nombre}

    # ---------- Vertical I: decisiones en tiempo real ----------
    @app.post("/v1/transacciones", status_code=202, response_model=TransaccionAceptada,
              tags=["tiempo real"], dependencies=protegido)
    def recibir_transaccion(tx: TransaccionIn, request: Request):
        tx_id = str(uuid.uuid4())
        ts = dt.datetime.now(dt.timezone.utc).isoformat()
        mensaje = {"transaccion_id": tx_id, "ts_evento": ts, **tx.model_dump()}
        try:
            request.app.state.publicador.publicar(mensaje)
        except Exception as e:
            log.error(json.dumps({"severity": "ERROR", "evento": "publicacion_fallida",
                                  "detalle": str(e)}))
            raise HTTPException(status_code=503, detail="No se pudo encolar la transaccion")
        score, motivos = reglas.score_preliminar(tx.tipo, tx.monto, tx.saldo_ini_origen,
                                                 tx.saldo_fin_origen)
        return TransaccionAceptada(
            transaccion_id=tx_id, ts_evento=ts, consultar_en=f"/v1/transacciones/{tx_id}",
            decision_preliminar=DecisionPreliminar(decision=reglas.decision(score),
                                                   score_riesgo=score, motivos=motivos))

    @app.get("/v1/transacciones/{transaccion_id}", response_model=DecisionTransaccion,
             tags=["tiempo real"], dependencies=protegido)
    def decision_transaccion(request: Request, transaccion_id: uuid.UUID):
        fila = request.app.state.analitica.decision_transaccion(str(transaccion_id))
        if fila is None:
            raise HTTPException(status_code=404,
                                detail="Transaccion no encontrada o aun en proceso")
        return DecisionTransaccion(
            transaccion_id=fila["transaccion_id"], decision=fila["decision"],
            score_riesgo=fila["score_riesgo"],
            motivos=[m for m in (fila.get("motivos") or "").split(",") if m],
            latencia_pipeline_ms=fila["latencia_ms"], tipo=fila["tipo"], monto=fila["monto"],
            cliente_origen_id=fila["cliente_origen_id"], distrito_id=fila["distrito_id"],
            segmento_etario=fila["segmento_etario"], ts_evento=str(fila["ts_evento"]),
            ts_proceso=str(fila["ts_proceso"]))

    @app.get("/v1/clientes/{cliente_id}/riesgo", response_model=RiesgoCliente,
             tags=["tiempo real"], dependencies=protegido)
    @app.get("/v1/clientes/{cliente_id}/perfil", response_model=RiesgoCliente,
             tags=["tiempo real"], dependencies=protegido, include_in_schema=False)
    def riesgo_cliente(request: Request,
                       cliente_id: str = Path(pattern=r"^C\d{4,12}$")):
        st = request.app.state
        clave = f"perfil:{cliente_id}"
        perfil, fuente = st.cache.obtener(clave), st.cache.nombre
        if perfil is None:
            perfil, fuente = st.perfiles.obtener(cliente_id), "bigtable"
            if perfil is None:
                raise HTTPException(status_code=404, detail="Cliente sin transacciones recientes")
            st.cache.guardar(clave, perfil, st.cfg.ttl_perfil_s)
        score = int(perfil["score_riesgo"]) if "score_riesgo" in perfil else None
        d = reglas.distrito(cliente_id)
        return RiesgoCliente(
            cliente_id=cliente_id, distrito_id=d, distrito_nombre=reglas.DISTRITOS[d],
            segmento_etario=reglas.segmento(cliente_id), nivel_riesgo=reglas.nivel_riesgo(score),
            ultima_decision=perfil.get("decision"), ultimo_score=score,
            ultimo_monto=float(perfil["monto"]) if "monto" in perfil else None,
            ultimo_canal=perfil.get("canal"), ultima_transaccion_ts=perfil.get("ts_evento"),
            historial_decisiones=perfil.get("historial", []), fuente=fuente)

    @app.get("/v1/alertas", tags=["tiempo real"], dependencies=protegido)
    def alertas(request: Request, limite: int = Query(default=10, ge=1, le=100),
                decision: str | None = Query(default=None, pattern="^(REVISAR|BLOQUEAR)$")):
        filas = request.app.state.analitica.alertas(limite, decision)
        for f in filas:
            f["motivos"] = [m for m in (f.get("motivos") or "").split(",") if m]
            f["distrito_nombre"] = reglas.DISTRITOS.get(f.get("distrito_id"))
        return {"n_alertas": len(filas), "decision": decision or "REVISAR o BLOQUEAR",
                "alertas": filas}

    # ---------- Verticales II y III: analitica gold ----------
    def con_cache(request: Request, clave: str, funcion):
        st = request.app.state
        valor = st.cache.obtener(clave)
        if valor is not None:
            return valor, st.cache.nombre
        valor = funcion()
        if valor:
            st.cache.guardar(clave, valor, st.cfg.ttl_analitica_s)
        return valor, "bigquery"

    @app.get("/v1/liquidez/{distrito_id}", tags=["analitica"], dependencies=protegido)
    def plan_liquidez(request: Request, distrito_id: int = Path(ge=1, le=8)):
        plan, fuente = con_cache(request, f"liquidez:{distrito_id}",
                                 lambda: request.app.state.analitica.plan_liquidez(distrito_id))
        if not plan:
            raise HTTPException(status_code=404, detail="Plan de liquidez no disponible")
        total = round(sum(p["efectivo_a_provisionar"] for p in plan), 2)
        return {"distrito_id": distrito_id, "distrito_nombre": reglas.DISTRITOS[distrito_id],
                "horizonte_dias": len(plan), "efectivo_total_a_provisionar": total,
                "plan": plan, "fuente": fuente}

    @app.get("/v1/kpis", tags=["analitica"], dependencies=protegido)
    def kpis_rango(request: Request, fecha: dt.date | None = None,
                   ultimos: int = Query(default=7, ge=1, le=90)):
        if fecha is not None:
            return kpis(request, fecha)
        datos, fuente = con_cache(request, f"kpis:ultimos:{ultimos}",
                                  lambda: request.app.state.analitica.kpis_ultimos(ultimos))
        if not datos:
            raise HTTPException(status_code=404, detail="Sin KPIs disponibles")
        return {"dias": len(datos), "kpis": datos, "fuente": fuente}

    @app.get("/v1/distritos", tags=["analitica"], dependencies=protegido)
    def distritos(request: Request):
        try:
            filas, fuente = con_cache(request, "distritos",
                                      lambda: request.app.state.analitica.distritos())
        except Exception as e:  # sin BigQuery se responde el catalogo base, sin tipo de zona
            log.warning(json.dumps({"severity": "WARNING", "evento": "distritos_sin_bq",
                                    "detalle": str(e)}))
            filas, fuente = [], "catalogo"
        if not filas:
            filas = [{"distrito_id": k, "distrito_nombre": v} for k, v in reglas.DISTRITOS.items()]
            fuente = "catalogo"
        return {"distritos": filas, "fuente": fuente}

    @app.get("/v1/kpis/{fecha}", tags=["analitica"], dependencies=protegido)
    def kpis(request: Request, fecha: dt.date):
        datos, fuente = con_cache(request, f"kpis:{fecha.isoformat()}",
                                  lambda: request.app.state.analitica.kpis_dia(fecha))
        if not datos:
            raise HTTPException(status_code=404, detail="Sin KPIs para esa fecha")
        return {**datos, "fuente": fuente}

    @app.get("/v1/operacion/ultimos-minutos", tags=["analitica"], dependencies=protegido)
    def operacion(request: Request, minutos: int = Query(default=15, ge=1, le=120)):
        filas = request.app.state.analitica.operacion_ultimos_minutos(minutos)
        return {"minutos": minutos, "n_ventanas": len(filas),
                "n_tx": sum(f["n_tx"] for f in filas),
                "n_alertas": sum(f["n_alertas"] for f in filas), "detalle": filas}

    return app


app = crear_app()
