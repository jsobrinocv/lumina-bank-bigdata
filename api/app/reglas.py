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
