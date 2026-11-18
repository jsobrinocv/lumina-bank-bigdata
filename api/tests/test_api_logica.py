"""Pruebas de la logica de la API que no requieren Google Cloud.

Ejecutar desde api/:  python -m pytest -q tests
"""
import hashlib
import sys
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import reglas  # noqa: E402
from app.cache import CacheMemoria  # noqa: E402
from app.modelos import TransaccionIn  # noqa: E402


def h_pipeline(texto, inicio, largo=8):
    """Copia literal de la funcion del pipeline de Dataflow (streaming/pipeline_streaming.py)."""
    return int(hashlib.md5(texto.encode("utf-8")).hexdigest()[inicio - 1:inicio - 1 + largo], 16)


@pytest.mark.parametrize("cliente", ["C1231006815", "C1666544295", "C1000000002", "C9"])
def test_distrito_y_segmento_iguales_al_pipeline(cliente):
    assert reglas.distrito(cliente) == h_pipeline(cliente, 1) % 8 + 1
    u = h_pipeline(cliente, 9) % 100
    esperado = "18-25" if u < 20 else "26-40" if u < 55 else "41-60" if u < 85 else "60+"
    assert reglas.segmento(cliente) == esperado


def test_nombres_de_distrito_iguales_a_dim_distrito():
    spark = Path(__file__).resolve().parents[2] / "spark" / "02_silver_to_gold.py"
    if not spark.exists():
        pytest.skip("se ejecuta dentro del repositorio completo")
    texto = spark.read_text(encoding="utf-8")
    for i, nombre in reglas.DISTRITOS.items():
        assert f'({i}, "{nombre}"' in texto


def test_distritos_cubren_los_8_valores():
    vistos = {reglas.distrito(f"C{i:010d}") for i in range(500)}
    assert vistos == set(range(1, 9))
    assert set(reglas.DISTRITOS) == vistos


@pytest.mark.parametrize("score,nivel", [(None, "SIN_HISTORIAL"), (0, "BAJO"), (49, "BAJO"),
                                         (50, "MEDIO"), (79, "MEDIO"), (80, "ALTO"), (100, "ALTO")])
def test_nivel_riesgo_usa_umbrales_del_pipeline(score, nivel):
    assert reglas.nivel_riesgo(score) == nivel


def test_cache_memoria_guarda_y_expira():
    c = CacheMemoria()
    c.guardar("k", {"a": 1}, ttl_s=1)
    assert c.obtener("k") == {"a": 1}
    c._datos["k"] = (time.monotonic() - 1, c._datos["k"][1])  # forzar vencimiento
    assert c.obtener("k") is None


def test_cache_memoria_respeta_el_maximo():
    c = CacheMemoria(max_items=2)
    for k in ("a", "b", "c"):
        c.guardar(k, k, ttl_s=60)
    assert c.obtener("a") is None and c.obtener("c") == "c"


def test_transaccion_valida_y_redondeo():
    tx = TransaccionIn(tipo="TRANSFER", monto=181000.456, cliente_origen_id="C1000000002",
                       cliente_destino_id="C2000000002", saldo_ini_origen=181000,
                       saldo_fin_origen=50000)
    assert tx.monto == 181000.46 and tx.canal == "APP" and tx.etiqueta_fraude == 0


@pytest.mark.parametrize("cambio", [{"tipo": "BITCOIN"}, {"monto": -5},
                                    {"cliente_origen_id": "X123"},
                                    {"cliente_origen_id": "C1; DROP TABLE x"},
                                    {"canal": "WEB"}])
def test_transaccion_invalida_se_rechaza(cambio):
    base = dict(tipo="PAYMENT", monto=10, cliente_origen_id="C1000000001",
                cliente_destino_id="M2000000001")
    with pytest.raises(ValidationError):
        TransaccionIn(**{**base, **cambio})


def test_transacciones_de_la_demo_obtienen_la_decision_esperada():
    """Replica el score del pipeline para asegurar que la demo muestre APROBAR y BLOQUEAR."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo"))
    import demo_e2e

    def score(tx):
        s = 0
        if tx["tipo"] in ("TRANSFER", "CASH_OUT"):
            s += 30
            si, m, sf = tx["saldo_ini_origen"], tx["monto"], tx["saldo_fin_origen"]
            if si > 0 and m >= si:
                s += 35
            if abs(si - m - sf) > 1:
                s += 15
        return s  # sin la regla del percentil 95 (solo puede subir el score)

    assert score(demo_e2e.TX_NORMAL) < reglas.UMBRAL_REVISAR
    assert score(demo_e2e.TX_FRAUDE) >= reglas.UMBRAL_BLOQUEAR
