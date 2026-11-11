"""
Lumina Bank - Pipeline de tiempo real (Apache Beam sobre Dataflow)

Pub/Sub -> parseo -> enriquecimiento (distrito/segmento + patron historico de BigQuery)
        -> score de riesgo y decision (APROBAR / REVISAR / BLOQUEAR)
        -> BigQuery (cada transaccion)            lumina_rt.rt_transaccion_scored_v1
        -> ventana fija 60 s por distrito y tipo  lumina_rt.rt_agg_minuto_v1
        -> Bigtable (opcional, perfil del cliente para consulta en milisegundos)
"""
import argparse
import hashlib
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions
from apache_beam.transforms import window

FMT = "%Y-%m-%d %H:%M:%S.%f"
RIESGOSOS = ("TRANSFER", "CASH_OUT")


# --- Misma formula de hash que el job Spark 02 (batch y streaming consistentes) ---
def h(texto, inicio, largo=8):
    return int(hashlib.md5(texto.encode("utf-8")).hexdigest()[inicio - 1:inicio - 1 + largo], 16)


def distrito(cliente_id):
    return h(cliente_id, 1) % 8 + 1


def segmento(cliente_id):
    u = h(cliente_id, 9) % 100
    return "18-25" if u < 20 else "26-40" if u < 55 else "41-60" if u < 85 else "60+"


def parsear(mensaje_bytes):
    try:
        yield json.loads(mensaje_bytes.decode("utf-8"))
    except Exception as e:  # mensajes corruptos no detienen el pipeline
        logging.warning("Mensaje descartado: %s", e)


class EnriquecerYCalificar(beam.DoFn):
    """Carga una vez por worker el patron de gasto aprendido en batch y califica cada tx."""

    def __init__(self, project, tabla_patron):
        self.project = project
        self.tabla_patron = tabla_patron
        self.patron = {}

    def setup(self):
        from google.cloud import bigquery
        cliente = bigquery.Client(project=self.project)
        q = f"SELECT tipo, segmento_etario, monto_p95, monto_promedio FROM `{self.tabla_patron}`"
        self.patron = {(r.tipo, r.segmento_etario): (r.monto_p95, r.monto_promedio)
                       for r in cliente.query(q).result()}
        logging.info("Patron de gasto cargado: %d combinaciones", len(self.patron))

    def process(self, m):
        ahora = datetime.now(timezone.utc)
        try:
            tipo = m["tipo"]
            monto = float(m["monto"])
            cid = m["cliente_origen_id"]
            si = float(m.get("saldo_ini_origen") or 0)
            sf = float(m.get("saldo_fin_origen") or 0)
            ts_ev = datetime.fromisoformat(m["ts_evento"])
        except Exception as e:
            logging.warning("Transaccion invalida %s: %s", m, e)
            return

        seg = segmento(cid)
        p95, _ = self.patron.get((tipo, seg), (None, None))

        score, motivos = 0, []
        if tipo in RIESGOSOS:
            score += 30; motivos.append("tipo_riesgoso")
            if si > 0 and monto >= si:
                score += 35; motivos.append("vacia_cuenta")
            if abs(si - monto - sf) > 1:
                score += 15; motivos.append("descuadre_saldo")
        if p95 and monto > p95:
            score += 20; motivos.append("monto_sobre_p95_historico")
        score = min(score, 100)
        decision = "BLOQUEAR" if score >= 80 else "REVISAR" if score >= 50 else "APROBAR"

        yield {
            "transaccion_id": m.get("transaccion_id"),
            "ts_evento": ts_ev.astimezone(timezone.utc).strftime(FMT),
            "ts_proceso": ahora.strftime(FMT),
            "latencia_ms": int((ahora - ts_ev).total_seconds() * 1000),
            "tipo": tipo,
            "monto": monto,
            "cliente_origen_id": cid,
            "cliente_destino_id": m.get("cliente_destino_id"),
            "distrito_id": distrito(cid),
            "segmento_etario": seg,
            "canal": m.get("canal"),
            "score_riesgo": score,
            "decision": decision,
            "motivos": ",".join(motivos),
            "etiqueta_fraude": int(m.get("etiqueta_fraude", 0)),
        }


def sumar_tuplas(valores):
    n = m = a = 0
    for v in valores:
        n += v[0]; m += v[1]; a += v[2]
    return (n, m, a)


class FormatearVentana(beam.DoFn):
    def process(self, kv, w=beam.DoFn.WindowParam):
        (dist, tipo), (n, monto, alertas) = kv
        yield {
            "ventana_inicio": w.start.to_utc_datetime().strftime(FMT),
            "ventana_fin": w.end.to_utc_datetime().strftime(FMT),
            "distrito_id": dist, "tipo": tipo,
            "n_tx": n, "monto_total": round(monto, 2), "alertas": a,
        }


class EscribirBigtable(beam.DoFn):
    """Guarda el ultimo estado de cada cliente (row key = cliente_id)."""

    def __init__(self, project, instance, table):
        self.project, self.instance, self.table_id = project, instance, table

    def setup(self):
        from google.cloud import bigtable
        self.tabla = bigtable.Client(project=self.project).instance(self.instance).table(self.table_id)

    def process(self, r):
        fila = self.tabla.direct_row(r["cliente_origen_id"].encode("utf-8"))
        ts = datetime.now(timezone.utc)
        for col in ("monto", "decision", "score_riesgo", "distrito_id", "ts_evento", "canal"):
            fila.set_cell("perfil", f"ultimo_{col}".encode(), str(r[col]).encode(), timestamp=ts)
        fila.commit()


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_subscription", required=True)
    ap.add_argument("--tabla_patron", required=True, help="proyecto.dataset.tabla")
    ap.add_argument("--tabla_scored", required=True, help="proyecto:dataset.tabla")
    ap.add_argument("--tabla_agg", required=True, help="proyecto:dataset.tabla")
    ap.add_argument("--bigtable_instance", default="")
    ap.add_argument("--bigtable_table", default="perfil_cliente")
    args, beam_args = ap.parse_known_args()

    opts = PipelineOptions(beam_args, streaming=True, save_main_session=True)
    project = opts.view_as(GoogleCloudOptions).project

    escribir_bq = dict(
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS)

    with beam.Pipeline(options=opts) as p:
        tx = (p
              | "LeerPubSub" >> beam.io.ReadFromPubSub(subscription=args.input_subscription)
              | "Parsear" >> beam.FlatMap(parsear)
              | "EnriquecerYCalificar" >> beam.ParDo(EnriquecerYCalificar(project, args.tabla_patron)))

        tx | "BQ_TxCalificadas" >> beam.io.WriteToBigQuery(args.tabla_scored, **escribir_bq)

        (tx
         | "Ventana60s" >> beam.WindowInto(window.FixedWindows(60))
         | "ClaveDistritoTipo" >> beam.Map(
               lambda r: ((r["distrito_id"], r["tipo"]),
                          (1, r["monto"], 0 if r["decision"] == "APROBAR" else 1)))
         | "Agregar" >> beam.CombinePerKey(sumar_tuplas)
         | "FormatoVentana" >> beam.ParDo(FormatearVentana())
         | "BQ_AggMinuto" >> beam.io.WriteToBigQuery(args.tabla_agg, **escribir_bq))

        if args.bigtable_instance:
            tx | "BigtablePerfil" >> beam.ParDo(
                EscribirBigtable(project, args.bigtable_instance, args.bigtable_table))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
