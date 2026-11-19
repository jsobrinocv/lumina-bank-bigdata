"""Reglas de negocio compartidas con los pipelines de las Entregas 2 y 3.

La asignacion de distrito y segmento usa la MISMA formula hash que el job Spark
02_silver_to_gold.py y el pipeline de Dataflow, de modo que batch, tiempo real
y la API ven al mismo cliente en el mismo distrito y segmento.
"""
import hashlib

# Mismos ids y nombres que gold_dim_distrito_v1 (spark/02_silver_to_gold.py)
DISTRITOS = {
    1: "Distrito Central", 2: "Puerto Norte", 3: "Valle Sur", 4: "Las Colinas",
    5: "Ribera Este", 6: "Nuevo Horizonte", 7: "San Data", 8: "Altos del Oeste",
}

# Umbrales del score de reglas del pipeline de tiempo real (Entrega 2, seccion 6.7)
UMBRAL_BLOQUEAR = 80
UMBRAL_REVISAR = 50
# Tipos que el pipeline considera riesgosos (streaming/pipeline_streaming.py)
RIESGOSOS = ("TRANSFER", "CASH_OUT")


def h(texto: str, inicio: int, largo: int = 8) -> int:
    return int(hashlib.md5(texto.encode("utf-8")).hexdigest()[inicio - 1:inicio - 1 + largo], 16)


def distrito(cliente_id: str) -> int:
    return h(cliente_id, 1) % 8 + 1


def segmento(cliente_id: str) -> str:
    u = h(cliente_id, 9) % 100
    return "18-25" if u < 20 else "26-40" if u < 55 else "41-60" if u < 85 else "60+"


def nivel_riesgo(score: int | None) -> str:
    """Traduce el ultimo score del cliente a un nivel legible para la App."""
    if score is None:
        return "SIN_HISTORIAL"
    if score >= UMBRAL_BLOQUEAR:
        return "ALTO"
    if score >= UMBRAL_REVISAR:
        return "MEDIO"
    return "BAJO"


def decision(score: int) -> str:
    return "BLOQUEAR" if score >= UMBRAL_BLOQUEAR else "REVISAR" if score >= UMBRAL_REVISAR else "APROBAR"


def score_preliminar(tipo: str, monto: float, saldo_ini: float, saldo_fin: float) -> tuple[int, list[str]]:
    """Mismas reglas del pipeline de Dataflow, sin la del percentil 95 historico.

    Sirve para responder al instante en el POST. La decision oficial es la de Dataflow
    (GET /v1/transacciones/{id}); como la regla omitida solo suma puntos, la oficial
    puede ser igual o mas estricta, nunca mas permisiva.
    """
    score, motivos = 0, []
    if tipo in RIESGOSOS:
        score += 30; motivos.append("tipo_riesgoso")
        if saldo_ini > 0 and monto >= saldo_ini:
            score += 35; motivos.append("vacia_cuenta")
        if abs(saldo_ini - monto - saldo_fin) > 1:
            score += 15; motivos.append("descuadre_saldo")
    return min(score, 100), motivos
