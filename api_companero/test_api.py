"""Pruebas locales de la API sin conectarse a Google Cloud (clientes simulados).

Uso:  pip install -r requirements.txt pytest httpx && pytest -q test_api.py
"""
import os

os.environ.update(PROJECT_ID="proyecto-prueba", API_KEY="clave-prueba")

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402

H = {"X-API-Key": "clave-prueba"}


class _Celda:
    def __init__(self, v):
        self.value = v.encode()


class _Fila:
    cells = {"perfil": {b"ultimo_monto": [_Celda("181.0")], b"ultimo_decision": [_Celda("BLOQUEAR")],
                        b"ultimo_score_riesgo": [_Celda("80")]}}


class _TablaBT:
    def read_row(self, key):
        return _Fila() if key == b"C1231006815" else None


class _Futuro:
    def result(self, timeout=None):
        return "msg-123"


class _Publisher:
    def topic_path(self, p, t):
        return f"projects/{p}/topics/{t}"

    def publish(self, ruta, data, **attrs):
        assert attrs["origen"] == "api"
        return _Futuro()


def _consulta_falsa(sql, params=None, cache_clave=None):
    if "gold_patron_gasto_v1" in sql:
        return [{"tipo": "TRANSFER", "segmento_etario": s, "monto_p95": 150.0}
                for s in ("18-25", "26-40", "41-60", "60+")]
    if "gold_plan_liquidez_v1" in sql:
        return [{"distrito_id": 3, "distrito_nombre": "Valle Sur", "fecha": "2026-11-19",
                 "retiro_esperado": 100.0, "retiro_escenario_alto": 150.0, "efectivo_a_provisionar": 165.0}] * 7
    if "gold_kpi_operativo_diario_v1" in sql:
        return [{"fecha": "2026-11-01", "n_transacciones": 1000}]
    return []


main._clientes.update(bt=_TablaBT(), ps=_Publisher())
main.consulta = _consulta_falsa
cli = TestClient(main.app)


def test_salud_sin_clave():
    assert cli.get("/salud").json()["estado"] == "ok"


def test_requiere_clave():
    assert cli.get("/v1/distritos").status_code == 401


def test_transaccion_fraude_se_bloquea():
    tx = {"tipo": "TRANSFER", "monto": 181.0, "cliente_origen_id": "C1231006815",
          "saldo_ini_origen": 181.0, "saldo_fin_origen": 0.0}
    r = cli.post("/v1/transacciones", json=tx, headers=H)
    assert r.status_code == 202, r.text
    body = r.json()
    # 30 tipo riesgoso + 35 vacia cuenta + 20 sobre p95 = 85 -> BLOQUEAR
    assert body["decision_preliminar"] == "BLOQUEAR" and body["score_riesgo"] == 85


def test_transaccion_invalida():
    r = cli.post("/v1/transacciones", json={"tipo": "OTRO", "monto": -5, "cliente_origen_id": "X1"}, headers=H)
    assert r.status_code == 422


def test_perfil():
    r = cli.get("/v1/clientes/C1231006815/perfil", headers=H)
    assert r.status_code == 200 and r.json()["ultima_transaccion"]["decision"] == "BLOQUEAR"
    assert cli.get("/v1/clientes/C999/perfil", headers=H).status_code == 404


def test_liquidez_y_kpis():
    r = cli.get("/v1/liquidez/3", headers=H).json()
    assert r["efectivo_total_7_dias"] == 1155.0
    assert cli.get("/v1/liquidez/9", headers=H).status_code == 404
    assert cli.get("/v1/kpis?fecha=2026-11-01", headers=H).json()["n_transacciones"] == 1000
