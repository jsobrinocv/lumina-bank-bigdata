"""Cache delante de Bigtable y BigQuery.

- Produccion: Memorystore (Redis/Valkey) compartido por todas las instancias de Cloud Run.
- Prototipo o local: cache en memoria del contenedor (sin costo), misma interfaz.
"""
import json
import threading
import time
from typing import Any, Optional


class CacheMemoria:
    nombre = "memoria"

    def __init__(self, max_items: int = 10_000):
        self._datos: dict[str, tuple[float, str]] = {}
        self._lock = threading.Lock()
        self._max = max_items

    def obtener(self, clave: str) -> Optional[Any]:
        with self._lock:
            item = self._datos.get(clave)
            if item is None:
                return None
            vence, valor = item
            if vence < time.monotonic():
                del self._datos[clave]
                return None
            return json.loads(valor)

    def guardar(self, clave: str, valor: Any, ttl_s: int) -> None:
        with self._lock:
            if len(self._datos) >= self._max:
                self._datos.pop(next(iter(self._datos)))  # descarta la entrada mas antigua
            self._datos[clave] = (time.monotonic() + ttl_s, json.dumps(valor, default=str))


class CacheRedis:
    nombre = "memorystore"

    def __init__(self, host: str, port: int):
        import redis  # dependencia solo en produccion
        self._r = redis.Redis(host=host, port=port, socket_timeout=0.05,
                              socket_connect_timeout=0.2)

    def obtener(self, clave: str) -> Optional[Any]:
        try:
            valor = self._r.get(clave)
        except Exception:  # si la cache falla, la API sigue leyendo de la fuente
            return None
        return None if valor is None else json.loads(valor)

    def guardar(self, clave: str, valor: Any, ttl_s: int) -> None:
        try:
            self._r.setex(clave, ttl_s, json.dumps(valor, default=str))
        except Exception:
            pass


def crear_cache(redis_host: str, redis_port: int):
    return CacheRedis(redis_host, redis_port) if redis_host else CacheMemoria()
