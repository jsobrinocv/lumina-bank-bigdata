"""Acceso a datos: Bigtable (perfil en milisegundos), BigQuery (gold y tiempo real) y Pub/Sub.

Los clientes de Google Cloud son sincronos; FastAPI ejecuta los endpoints `def`
en un pool de hilos, por lo que no bloquean el servidor.
"""
import datetime as dt
import decimal
import json
from typing import Any, Optional

PERFIL_FAMILIA = "perfil"
PERFIL_COLUMNAS = ("monto", "decision", "score_riesgo", "distrito_id", "ts_evento", "canal")


def _a_json(valor: Any) -> Any:
    if isinstance(valor, (dt.datetime, dt.date)):
        return valor.isoformat()
    if isinstance(valor, decimal.Decimal):
        return float(valor)
    return valor


class PerfilRepo:
    """Ultimo estado de cada cliente, escrito por el pipeline de Dataflow (row key = cliente_id)."""

    def __init__(self, project: str, instance: str, table: str):
        from google.cloud import bigtable
        self._tabla = bigtable.Client(project=project).instance(instance).table(table)

    def obtener(self, cliente_id: str) -> Optional[dict]:
        fila = self._tabla.read_row(cliente_id.encode("utf-8"))
        if fila is None:
            return None
        celdas = fila.cells.get(PERFIL_FAMILIA, {})
        perfil = {}
        for col in PERFIL_COLUMNAS:
            versiones = celdas.get(f"ultimo_{col}".encode())
            if versiones:  # Bigtable devuelve la version mas reciente primero
                perfil[col] = versiones[0].value.decode("utf-8")
        # La familia guarda hasta 5 versiones: sirven como historial corto de decisiones
        perfil["historial"] = [
            {"decision": c.value.decode("utf-8"), "ts": c.timestamp.isoformat()}
            for c in celdas.get(b"ultimo_decision", [])
        ]
        return perfil


class AnaliticaRepo:
    """Consultas parametrizadas sobre BigQuery (capa gold y tablas de tiempo real)."""

    def __init__(self, project: str, ds_gold: str, ds_rt: str, max_bytes: int):
        from google.cloud import bigquery
        self._bq = bigquery
        self._cliente = bigquery.Client(project=project)
        self._p, self._gold, self._rt = project, ds_gold, ds_rt
        self._max_bytes = max_bytes

    def _consultar(self, sql: str, params: list) -> list[dict]:
        cfg = self._bq.QueryJobConfig(query_parameters=params,
                                      maximum_bytes_billed=self._max_bytes,
                                      use_query_cache=True)
        filas = self._cliente.query(sql, job_config=cfg).result()
        return [{k: _a_json(v) for k, v in dict(f).items()} for f in filas]

    def decision_transaccion(self, transaccion_id: str) -> Optional[dict]:
        # El filtro por ts_evento usa la particion diaria: solo se leen los 2 ultimos dias
        sql = f"""
            SELECT transaccion_id, decision, score_riesgo, motivos, latencia_ms, tipo, monto,
                   cliente_origen_id, distrito_id, segmento_etario, ts_evento, ts_proceso
            FROM `{self._p}.{self._rt}.rt_transaccion_scored_v1`
            WHERE ts_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
              AND transaccion_id = @id
            LIMIT 1"""
        filas = self._consultar(sql, [self._bq.ScalarQueryParameter("id", "STRING", transaccion_id)])
        return filas[0] if filas else None

    def plan_liquidez(self, distrito_id: int) -> list[dict]:
        sql = f"""
            SELECT distrito_id, distrito_nombre, fecha, retiro_esperado,
                   retiro_escenario_alto, efectivo_a_provisionar
            FROM `{self._p}.{self._gold}.gold_plan_liquidez_v1`
            WHERE distrito_id = @d
            ORDER BY fecha"""
        return self._consultar(sql, [self._bq.ScalarQueryParameter("d", "INT64", distrito_id)])

    def kpis_dia(self, fecha: dt.date) -> Optional[dict]:
        p = [self._bq.ScalarQueryParameter("f", "DATE", fecha)]
        kpi = self._consultar(f"""
            SELECT * FROM `{self._p}.{self._gold}.gold_kpi_operativo_diario_v1`
            WHERE fecha = @f""", p)
        if not kpi:
            return None
        productos = self._consultar(f"""
            SELECT producto, n_transacciones, monto, ingresos_estimados
            FROM `{self._p}.{self._gold}.gold_ingresos_producto_diario_v1`
            WHERE fecha = @f ORDER BY ingresos_estimados DESC""", p)
        return {**kpi[0], "ingresos_por_producto": productos}

    def alertas(self, limite: int, decision: Optional[str]) -> list[dict]:
        # Solo las 2 ultimas particiones diarias: la consulta lee poco aunque la tabla crezca
        sql = f"""
            SELECT transaccion_id, ts_evento, tipo, monto, cliente_origen_id, distrito_id,
                   decision, score_riesgo, motivos, latencia_ms
            FROM `{self._p}.{self._rt}.rt_transaccion_scored_v1`
            WHERE ts_evento >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
              AND decision != 'APROBAR'
              AND (@dec IS NULL OR decision = @dec)
            ORDER BY ts_evento DESC
            LIMIT @n"""
        return self._consultar(sql, [self._bq.ScalarQueryParameter("dec", "STRING", decision),
                                     self._bq.ScalarQueryParameter("n", "INT64", limite)])

    def kpis_ultimos(self, dias: int) -> list[dict]:
        sql = f"""
            SELECT * FROM `{self._p}.{self._gold}.gold_kpi_operativo_diario_v1`
            ORDER BY fecha DESC
            LIMIT @n"""
        return self._consultar(sql, [self._bq.ScalarQueryParameter("n", "INT64", dias)])

    def distritos(self) -> list[dict]:
        sql = f"""
            SELECT distrito_id, distrito_nombre, tipo_zona
            FROM `{self._p}.{self._gold}.gold_dim_distrito_v1`
            ORDER BY distrito_id"""
        return self._consultar(sql, [])

    def operacion_ultimos_minutos(self, minutos: int) -> list[dict]:
        sql = f"""
            SELECT ventana_inicio, distrito_id, tipo, n_tx, monto_total, n_alertas
            FROM `{self._p}.{self._rt}.rt_agg_minuto_v1`
            WHERE ventana_inicio >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE)
            ORDER BY ventana_inicio DESC, distrito_id, tipo"""
        return self._consultar(sql, [self._bq.ScalarQueryParameter("m", "INT64", minutos)])


class Publicador:
    """Publica transacciones en el mismo topico que usa el simulador: el pipeline no cambia."""

    def __init__(self, project: str, topic: str):
        from google.cloud import pubsub_v1
        self._pub = pubsub_v1.PublisherClient()
        self._topic = self._pub.topic_path(project, topic)

    def publicar(self, mensaje: dict) -> str:
        futuro = self._pub.publish(self._topic, json.dumps(mensaje).encode("utf-8"), origen="api")
        return futuro.result(timeout=10)
