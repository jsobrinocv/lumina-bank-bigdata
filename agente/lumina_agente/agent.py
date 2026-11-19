"""
Lumina Bank - Agente conversacional (Google ADK + Gemini)

Asistente para analistas de riesgo y operaciones. No accede directamente a las bases
de datos: consume la API de exposicion de la plataforma (Cloud Run), con la misma
clave y los mismos permisos que cualquier otra aplicacion consumidora.

Ejecucion (desde la carpeta agente/):   adk web --port 8000
"""
import json
import os
import urllib.error
import urllib.parse
import urllib.request

from google.adk.agents import Agent

API_URL = os.environ.get("LUMINA_API_URL", "").rstrip("/")
API_KEY = os.environ.get("LUMINA_API_KEY", "")
MODELO = os.environ.get("LUMINA_MODELO", "gemini-4-hydrogen")
NIVEL_RAZONAMIENTO = os.environ.get("LUMINA_RAZONAMIENTO", "").upper()  # LOW, MEDIUM, HIGH, XHIGH


def _config_generacion():
    """Nivel de razonamiento opcional (Gemini 4: LOW responde mas rapido en la demo)."""
    if not NIVEL_RAZONAMIENTO:
        return None
    try:
        from google.genai import types
        return types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_level=NIVEL_RAZONAMIENTO))
    except Exception as e:  # SDK antiguo sin thinking_level: se usa el valor por defecto del modelo
        print(f"[agente] No se aplico LUMINA_RAZONAMIENTO={NIVEL_RAZONAMIENTO}: {e}")
        return None


def _llamar(metodo: str, ruta: str, cuerpo: dict | None = None, params: dict | None = None) -> dict:
    """Llama a la API de Lumina y devuelve el JSON (o un dict con el error)."""
    if not API_URL:
        return {"error": "Falta la variable LUMINA_API_URL en el archivo .env"}
    url = API_URL + ruta
    if params:
        url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(url, data=datos, method=metodo,
                                 headers={"X-API-Key": API_KEY, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}", "detalle": e.read().decode(errors="ignore")}
    except Exception as e:  # red, tiempo de espera, etc.
        return {"error": str(e)}


# ---------------- Herramientas que el agente puede usar ----------------

def consultar_perfil_cliente(cliente_id: str) -> dict:
    """Devuelve la ultima transaccion evaluada en tiempo real para un cliente (monto, decision,
    score de riesgo, distrito, canal y hora), leida desde Bigtable.

    Args:
        cliente_id: identificador del cliente, con formato C seguido de 6 a 10 digitos (ej. C1231006815).
    """
    return _llamar("GET", f"/v1/clientes/{cliente_id}/perfil")


def listar_alertas(limite: int = 10, decision: str = "") -> dict:
    """Lista las ultimas transacciones de las ultimas 24 horas que el motor de riesgo envio a
    REVISAR o BLOQUEAR, con su score y los motivos de la alerta.

    Args:
        limite: cantidad maxima de alertas a devolver (1 a 200).
        decision: opcional, 'REVISAR' o 'BLOQUEAR' para filtrar; vacio para ambas.
    """
    return _llamar("GET", "/v1/alertas", params={"limite": limite, "decision": decision})


def plan_de_liquidez(distrito_id: int) -> dict:
    """Devuelve el plan de efectivo de los proximos 7 dias para un distrito: retiro esperado,
    escenario alto y efectivo a provisionar (pronostico de BigQuery ML con 10 % de colchon).

    Args:
        distrito_id: numero del distrito entre 1 y 8. Usa listar_distritos si el usuario da el nombre.
    """
    return _llamar("GET", f"/v1/liquidez/{distrito_id}")


def kpis_operativos(fecha: str = "", ultimos: int = 7) -> dict:
    """Devuelve los KPIs operativos diarios: numero de transacciones, monto, ingresos estimados,
    costo operativo por transaccion, tasa de fraude y recall del sistema legado.

    Args:
        fecha: opcional, dia en formato AAAA-MM-DD. Si se omite, devuelve los ultimos dias.
        ultimos: cantidad de dias recientes cuando no se indica fecha.
    """
    return _llamar("GET", "/v1/kpis", params={"fecha": fecha, "ultimos": ultimos})


def listar_distritos() -> list | dict:
    """Devuelve el catalogo de distritos de Lumina con su id, nombre y tipo de zona."""
    return _llamar("GET", "/v1/distritos")


def registrar_transaccion(tipo: str, monto: float, cliente_origen_id: str,
                          saldo_ini_origen: float, saldo_fin_origen: float,
                          canal: str = "APP", cliente_destino_id: str = "C0000000000") -> dict:
    """Registra una transaccion de prueba: la publica en Pub/Sub para que el pipeline la califique,
    y devuelve la decision preliminar (APROBAR, REVISAR o BLOQUEAR) con su score y motivos.
    Usala solo cuando el usuario pida explicitamente simular o registrar una transaccion.

    Args:
        tipo: CASH_IN, CASH_OUT, DEBIT, PAYMENT o TRANSFER.
        monto: monto de la operacion (mayor que 0).
        cliente_origen_id: cliente que origina la operacion (C + 6 a 10 digitos).
        saldo_ini_origen: saldo del cliente antes de la operacion.
        saldo_fin_origen: saldo del cliente despues de la operacion.
        canal: APP, CAJERO, AGENTE u OFICINA.
        cliente_destino_id: cliente que recibe la operacion.
    """
    cuerpo = dict(tipo=tipo, monto=monto, cliente_origen_id=cliente_origen_id,
                  cliente_destino_id=cliente_destino_id, saldo_ini_origen=saldo_ini_origen,
                  saldo_fin_origen=saldo_fin_origen, canal=canal)
    return _llamar("POST", "/v1/transacciones", cuerpo=cuerpo)


root_agent = Agent(
    name="asistente_lumina",
    model=MODELO,
    description="Asistente de riesgo y operaciones de Lumina Bank que consulta la plataforma de datos.",
    instruction="""Eres el Asistente de Riesgo y Operaciones de Lumina Bank. Respondes en espanol, de forma
breve y profesional, a analistas de fraude, de tesoreria y a la gerencia.

Reglas:
- Usa siempre las herramientas para obtener datos; nunca inventes cifras, clientes ni decisiones.
- Si el usuario menciona un distrito por su nombre, primero usa listar_distritos para obtener su id.
- Al explicar una alerta, traduce los motivos: tipo_riesgoso = transferencia o retiro (donde ocurre el fraude);
  vacia_cuenta = el monto vacia el saldo de la cuenta; descuadre_saldo = el saldo final no cuadra con la
  operacion; monto_sobre_p95_historico = el monto supera el 95 % de lo habitual para ese tipo y segmento.
- Score: 0-49 APROBAR, 50-79 REVISAR (verificacion adicional), 80-100 BLOQUEAR.
- Presenta montos con separador de miles y dos decimales; usa tablas cortas cuando compares varios valores.
- Solo registra transacciones si el usuario lo pide de forma explicita y aclara que es una prueba.
- Si una herramienta devuelve un error, explicalo en una frase y sugiere que hacer.""",
    tools=[consultar_perfil_cliente, listar_alertas, plan_de_liquidez, kpis_operativos,
           listar_distritos, registrar_transaccion],
    generate_content_config=_config_generacion(),
)
