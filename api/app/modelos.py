"""Contratos de entrada y salida de la API (validados con Pydantic)."""
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

TIPOS = ("CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER")
CANALES = ("APP", "CAJERO", "AGENTE", "OFICINA")
PATRON_CLIENTE = r"^C\d{4,12}$"
PATRON_DESTINO = r"^[CM]\d{4,12}$"


class TransaccionIn(BaseModel):
    """Transaccion enviada por la App o un cajero. Mismo esquema que el simulador (producer.py)."""
    tipo: Literal["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
    monto: float = Field(ge=0, le=100_000_000)
    cliente_origen_id: str = Field(pattern=PATRON_CLIENTE)
    cliente_destino_id: str = Field(pattern=PATRON_DESTINO)
    saldo_ini_origen: float = Field(default=0, ge=0)
    saldo_fin_origen: float = Field(default=0, ge=0)
    canal: Literal["APP", "CAJERO", "AGENTE", "OFICINA"] = "APP"
    # Solo para medir el acierto en la demo; en produccion no existe
    etiqueta_fraude: int = Field(default=0, ge=0, le=1)

    @field_validator("monto", "saldo_ini_origen", "saldo_fin_origen")
    @classmethod
    def dos_decimales(cls, v: float) -> float:
        return round(v, 2)


class TransaccionAceptada(BaseModel):
    transaccion_id: str
    estado: Literal["RECIBIDA"] = "RECIBIDA"
    ts_evento: str
    consultar_en: str


class DecisionTransaccion(BaseModel):
    transaccion_id: str
    decision: str
    score_riesgo: int
    motivos: list[str]
    latencia_pipeline_ms: int
    tipo: str
    monto: float
    cliente_origen_id: str
    distrito_id: int
    segmento_etario: str
    ts_evento: str
    ts_proceso: str


class RiesgoCliente(BaseModel):
    cliente_id: str
    distrito_id: int
    distrito_nombre: str
    segmento_etario: str
    nivel_riesgo: str
    ultima_decision: Optional[str] = None
    ultimo_score: Optional[int] = None
    ultimo_monto: Optional[float] = None
    ultimo_canal: Optional[str] = None
    ultima_transaccion_ts: Optional[str] = None
    historial_decisiones: list[dict] = []
    fuente: Literal["memorystore", "memoria", "bigtable"]
