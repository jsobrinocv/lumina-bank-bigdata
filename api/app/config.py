"""Configuracion de la API leida desde variables de entorno (Cloud Run)."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    project_id: str
    bt_instance: str
    bt_table: str
    topic: str
    ds_gold: str
    ds_rt: str
    redis_host: str
    redis_port: int
    ttl_perfil_s: int
    ttl_analitica_s: int
    max_bytes_consulta: int
    api_key: str


def cargar() -> Config:
    e = os.environ.get
    return Config(
        project_id=e("PROJECT_ID", ""),
        bt_instance=e("BT_INSTANCE", "lumina-rt--lumina-rt"),
        bt_table=e("BT_TABLE", "perfil_cliente"),
        topic=e("TOPIC", "lumina-transacciones"),
        ds_gold=e("DS_GOLD", "lumina_gold"),
        ds_rt=e("DS_RT", "lumina_rt"),
        # Memorystore: si REDIS_HOST esta vacio se usa una cache en memoria del contenedor
        redis_host=e("REDIS_HOST", ""),
        redis_port=int(e("REDIS_PORT", "6379")),
        # El perfil cambia con cada transaccion: TTL corto. Liquidez y KPIs cambian 1 vez al dia.
        ttl_perfil_s=int(e("TTL_PERFIL_S", "30")),
        ttl_analitica_s=int(e("TTL_ANALITICA_S", "3600")),
        # Tope de costo por consulta de BigQuery (1 GB): una consulta mal filtrada falla en vez de cobrar
        max_bytes_consulta=int(e("MAX_BYTES_CONSULTA", str(1024 ** 3))),
        # API key opcional, inyectada desde Secret Manager (--set-secrets API_KEY=...)
        # strip(): un secreto creado con `echo` u `openssl` puede traer un salto de linea final
        api_key=e("API_KEY", "").strip(),
    )
