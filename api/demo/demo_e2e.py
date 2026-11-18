"""
Lumina Bank - Demo end-to-end en vivo (Entrega 3).

Recorrido:  App (este script) -> API Cloud Run -> Pub/Sub -> Dataflow (score y decision)
            -> Bigtable + BigQuery -> API -> App

Requisitos: pipeline de Dataflow corriendo (Entrega 2) y API desplegada (api/deploy.sh).
Uso (Cloud Shell):
  python3 api/demo/demo_e2e.py --url $(gcloud run services describe lumina-api \
      --region us-central1 --format='value(status.url)')
Solo usa la libreria estandar de Python.
"""
import argparse
import json
import statistics
import subprocess
import time
import urllib.error
import urllib.request

AZUL, VERDE, ROJO, AMARILLO, GRIS, FIN = "\033[94m", "\033[92m", "\033[91m", "\033[93m", "\033[90m", "\033[0m"
COLOR_DECISION = {"APROBAR": VERDE, "REVISAR": AMARILLO, "BLOQUEAR": ROJO}

# Dos transacciones de ejemplo: una normal y una con el patron de fraude de PaySim
# (transferencia que vacia la cuenta y deja un saldo final que no cuadra).
TX_NORMAL = {"tipo": "PAYMENT", "monto": 250.0, "cliente_origen_id": "C1000000001",
             "cliente_destino_id": "M2000000001", "saldo_ini_origen": 5000.0,
             "saldo_fin_origen": 4750.0, "canal": "APP"}
TX_FRAUDE = {"tipo": "TRANSFER", "monto": 181000.0, "cliente_origen_id": "C1000000002",
             "cliente_destino_id": "C2000000002", "saldo_ini_origen": 181000.0,
             "saldo_fin_origen": 50000.0, "canal": "APP", "etiqueta_fraude": 1}


def comando(*args):
    return subprocess.run(args, capture_output=True, text=True, check=True).stdout.strip()


class Cliente:
    def __init__(self, url, token, api_key):
        self.url, self.h = url.rstrip("/"), {"Content-Type": "application/json"}
        if token:
            self.h["Authorization"] = f"Bearer {token}"
        if api_key:
            self.h["X-API-Key"] = api_key

    def llamar(self, metodo, ruta, cuerpo=None):
        datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
        req = urllib.request.Request(self.url + ruta, data=datos, headers=self.h, method=metodo)
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                estado, texto, srv = r.status, r.read().decode(), r.headers.get("X-Latencia-ms")
        except urllib.error.HTTPError as e:
            estado, texto, srv = e.code, e.read().decode(), e.headers.get("X-Latencia-ms")
        total = (time.perf_counter() - t0) * 1000
        try:
            cuerpo_resp = json.loads(texto)
        except json.JSONDecodeError:
            cuerpo_resp = texto
        return estado, cuerpo_resp, total, float(srv) if srv else None


def titulo(n, texto):
    print(f"\n{AZUL}[{n}] {texto}{FIN}")


def esperar_decision(api, tx_id, limite_s=60):
    inicio = time.perf_counter()
    while time.perf_counter() - inicio < limite_s:
        estado, cuerpo, _, _ = api.llamar("GET", f"/v1/transacciones/{tx_id}")
        if estado == 200:
            return cuerpo, time.perf_counter() - inicio
        time.sleep(1)
    return None, limite_s


def percentil(valores, p):
    v = sorted(valores)
    return v[min(len(v) - 1, int(round(p / 100 * (len(v) - 1))))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--repeticiones", type=int, default=50,
                    help="consultas de perfil para medir la latencia")
    ap.add_argument("--sin-token", action="store_true",
                    help="API publica detras del balanceador (sin token de identidad)")
    args = ap.parse_args()

    token = None if args.sin_token else comando("gcloud", "auth", "print-identity-token")
    api_key = comando("gcloud", "secrets", "versions", "access", "latest", "--secret=lumina-api-key")
    api = Cliente(args.url, token, api_key)

    titulo(1, "Salud del servicio")
    estado, cuerpo, ms, _ = api.llamar("GET", "/health")
    print(f"   {estado} {cuerpo}  ({ms:.0f} ms)")

    titulo(2, "La App envia dos transacciones a la API (POST /v1/transacciones)")
    ids = {}
    for nombre, tx in (("normal", TX_NORMAL), ("sospechosa", TX_FRAUDE)):
        estado, cuerpo, ms, _ = api.llamar("POST", "/v1/transacciones", tx)
        if estado != 202:
            raise SystemExit(f"   {ROJO}La API respondio {estado}: {cuerpo}{FIN}")
        ids[nombre] = (cuerpo["transaccion_id"], tx["cliente_origen_id"])
        print(f"   {nombre:<10} {tx['tipo']:<8} {tx['monto']:>12,.2f} -> {estado} "
              f"{cuerpo['estado']}  id={cuerpo['transaccion_id'][:8]}...  ({ms:.0f} ms)")

    titulo(3, "Pub/Sub -> Dataflow califica y decide; la API devuelve la decision")
    for nombre, (tx_id, _) in ids.items():
        d, espera = esperar_decision(api, tx_id)
        if d is None:
            print(f"   {nombre}: sin decision en 60 s (revisar que el job de Dataflow este activo)")
            continue
        c = COLOR_DECISION.get(d["decision"], "")
        print(f"   {nombre:<10} {c}{d['decision']:<9}{FIN} score={d['score_riesgo']:>3}  "
              f"motivos={','.join(d['motivos']) or '-'}")
        print(f"   {GRIS}{'':<10} latencia del pipeline {d['latencia_pipeline_ms']} ms · "
              f"visible en la API en {espera:.1f} s · distrito {d['distrito_id']} · "
              f"segmento {d['segmento_etario']}{FIN}")

    titulo(4, "Perfil de riesgo del cliente sospechoso (Bigtable y luego cache)")
    cliente = ids["sospechosa"][1]
    for intento in (1, 2):
        estado, cuerpo, ms, srv = api.llamar("GET", f"/v1/clientes/{cliente}/riesgo")
        if estado != 200:
            print(f"   {estado} {cuerpo}")
            break
        print(f"   consulta {intento}: nivel={cuerpo['nivel_riesgo']} "
              f"ultima_decision={cuerpo['ultima_decision']} fuente={cuerpo['fuente']} "
              f"servidor={srv} ms total={ms:.0f} ms")

    titulo(5, f"Latencia de la API en {args.repeticiones} consultas de perfil")
    serv, total = [], []
    for _ in range(args.repeticiones):
        _, _, ms, srv = api.llamar("GET", f"/v1/clientes/{cliente}/riesgo")
        total.append(ms)
        if srv is not None:
            serv.append(srv)
    if serv:
        print(f"   servidor: p50={statistics.median(serv):.1f} ms  p95={percentil(serv, 95):.1f} ms")
    print(f"   extremo a extremo desde Cloud Shell: p50={statistics.median(total):.0f} ms  "
          f"p95={percentil(total, 95):.0f} ms")

    titulo(6, "Plan de liquidez (BigQuery ML) y KPIs operativos (capa gold)")
    estado, cuerpo, ms, _ = api.llamar("GET", "/v1/liquidez/1")
    if estado == 200:
        print(f"   {cuerpo['distrito_nombre']}: provisionar {cuerpo['efectivo_total_a_provisionar']:,.0f} "
              f"u.m. en {cuerpo['horizonte_dias']} dias (fuente {cuerpo['fuente']}, {ms:.0f} ms)")
    estado, cuerpo, ms, _ = api.llamar("GET", "/v1/kpis/2026-09-10")
    if estado == 200:
        print(f"   10-set-2026: {cuerpo['n_transacciones']:,} tx · tasa de fraude "
              f"{cuerpo['tasa_fraude']:.4%} · costo por tx US$ {cuerpo['costo_operativo_por_tx']} "
              f"({ms:.0f} ms)")

    titulo(7, "Operacion de los ultimos 15 minutos (agregados de Dataflow)")
    estado, cuerpo, ms, _ = api.llamar("GET", "/v1/operacion/ultimos-minutos?minutos=15")
    if estado == 200:
        print(f"   {cuerpo['n_tx']} transacciones · {cuerpo['n_alertas']} alertas · "
              f"{cuerpo['n_ventanas']} ventanas ({ms:.0f} ms)")
    print(f"\n{VERDE}Demo completa.{FIN}")


if __name__ == "__main__":
    main()
