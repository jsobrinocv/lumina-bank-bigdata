"""
Lumina Bank - Simulador de la App/cajeros: publica transacciones en Pub/Sub.

Uso:
  python producer.py --project $PROJECT_ID --topic $TOPIC \
                     --archivo muestra_stream.csv --tps 20 --max 6000
"""
import argparse
import csv
import json
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

ap = argparse.ArgumentParser()
ap.add_argument("--project", required=True)
ap.add_argument("--topic", required=True)
ap.add_argument("--archivo", default="muestra_stream.csv")
ap.add_argument("--tps", type=float, default=20, help="transacciones por segundo")
ap.add_argument("--max", type=int, default=0, help="0 = enviar todo el archivo una vez")
ap.add_argument("--loop", action="store_true", help="repetir el archivo indefinidamente")
args = ap.parse_args()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(args.project, args.topic)

with open(args.archivo, newline="") as f:
    filas = list(csv.DictReader(f))
print(f"{len(filas)} filas cargadas; publicando en {topic_path} a {args.tps} tx/s")

enviados, pendientes, inicio = 0, [], time.time()
try:
    while True:
        for r in filas:
            msg = {
                "transaccion_id": str(uuid.uuid4()),
                "ts_evento": datetime.now(timezone.utc).isoformat(),
                "tipo": r["tipo"],
                "monto": float(r["monto"]),
                "cliente_origen_id": r["cliente_origen_id"],
                "cliente_destino_id": r["cliente_destino_id"],
                "saldo_ini_origen": float(r["saldo_ini_origen"] or 0),
                "saldo_fin_origen": float(r["saldo_fin_origen"] or 0),
                "canal": r["canal"],
                "etiqueta_fraude": int(r["es_fraude"]),  # solo para medir el acierto en la demo
            }
            pendientes.append(publisher.publish(topic_path, json.dumps(msg).encode("utf-8"),
                                                origen="simulador"))
            enviados += 1
            if enviados % 500 == 0:
                for p in pendientes:
                    p.result()
                pendientes = []
                print(f"  {enviados} enviados ({enviados / (time.time() - inicio):.1f} tx/s)")
            if args.max and enviados >= args.max:
                break
            time.sleep(1.0 / args.tps)
        if not args.loop or (args.max and enviados >= args.max):
            break
except KeyboardInterrupt:
    print("Interrumpido por el usuario")
finally:
    for p in pendientes:
        p.result()
    print(f"Total publicado: {enviados}")
