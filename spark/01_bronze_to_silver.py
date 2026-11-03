"""
Lumina Bank - Job Spark 1: BRONZE -> SILVER
Lee el CSV crudo de PaySim (bronze), valida calidad, deduplica, tipa,
deriva variables de negocio y escribe Parquet particionado por fecha (silver).

Ejecucion (Dataproc):
  gcloud dataproc jobs submit pyspark gs://$BUCKET/scripts/01_bronze_to_silver.py \
     --cluster=$CLUSTER --region=$REGION -- --bucket=$BUCKET
"""
import argparse
import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

TIPOS_VALIDOS = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]

ap = argparse.ArgumentParser()
ap.add_argument("--bucket", required=True)
ap.add_argument("--fecha_base", default="2026-09-01 00:00:00",
                help="Fecha/hora que corresponde al step 1 de PaySim (1 step = 1 hora)")
args = ap.parse_args()

BRONZE = f"gs://{args.bucket}/bronze/paysim/"
SILVER = f"gs://{args.bucket}/silver/transacciones/"
CUARENTENA = f"gs://{args.bucket}/quarantine/paysim/"
CALIDAD = f"gs://{args.bucket}/quality/bronze_to_silver/"

spark = (SparkSession.builder.appName("lumina_bronze_to_silver")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
         .getOrCreate())

# 1) Lectura de bronze (todo como texto; se tipa por nombre de columna -> robusto al orden)
raw = spark.read.option("header", True).csv(BRONZE)
total_bronze = raw.count()
print(f"[bronze] filas leidas: {total_bronze}")

df = raw.select(
    F.col("step").cast("int").alias("step"),
    F.upper(F.trim(F.col("type"))).alias("tipo"),
    F.col("amount").cast("double").alias("monto"),
    F.trim(F.col("nameOrig")).alias("cliente_origen_id"),
    F.col("oldbalanceOrg").cast("double").alias("saldo_ini_origen"),
    F.col("newbalanceOrig").cast("double").alias("saldo_fin_origen"),
    F.trim(F.col("nameDest")).alias("cliente_destino_id"),
    F.col("oldbalanceDest").cast("double").alias("saldo_ini_destino"),
    F.col("newbalanceDest").cast("double").alias("saldo_fin_destino"),
    F.col("isFraud").cast("int").alias("es_fraude"),
    F.col("isFlaggedFraud").cast("int").alias("alerta_sistema_legado"),
)

# 2) Reglas de calidad (las filas que no cumplen van a cuarentena, no se pierden)
regla_ok = F.coalesce(
    F.col("step").isNotNull() & (F.col("step") >= 1)
    & F.col("tipo").isin(TIPOS_VALIDOS)
    & F.col("monto").isNotNull() & (F.col("monto") >= 0)
    & F.col("cliente_origen_id").isNotNull() & F.col("cliente_destino_id").isNotNull()
    & F.col("es_fraude").isin(0, 1),
    F.lit(False))

df = df.withColumn("_ok", regla_ok)
rechazados = df.filter(~F.col("_ok")).drop("_ok")
validos = df.filter(F.col("_ok")).drop("_ok")

n_rechazados = rechazados.count()
rechazados.withColumn("_motivo", F.lit("regla_calidad")) \
    .write.mode("overwrite").parquet(CUARENTENA)

# 3) Deduplicacion exacta + llave tecnica
n_antes = validos.count()
validos = validos.dropDuplicates()
n_despues = validos.count()
cols_llave = ["step", "tipo", "monto", "cliente_origen_id", "saldo_ini_origen",
              "saldo_fin_origen", "cliente_destino_id", "saldo_ini_destino", "saldo_fin_destino"]
validos = validos.withColumn(
    "transaccion_id", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in cols_llave]), 256))

# 4) Enriquecimiento: tiempo de negocio y variables derivadas
ts_evento = (F.unix_timestamp(F.lit(args.fecha_base)) + (F.col("step") - 1) * 3600).cast("timestamp")
riesgosos = F.col("tipo").isin("TRANSFER", "CASH_OUT")

silver = (validos
    .withColumn("ts_evento", ts_evento)
    .withColumn("fecha", F.to_date("ts_evento"))
    .withColumn("hora", F.hour("ts_evento"))
    .withColumn("tipo_destino",
                F.when(F.col("cliente_destino_id").startswith("M"), "COMERCIO").otherwise("CLIENTE"))
    # Descuadre de saldo en el origen (debitos restan, CASH_IN suma)
    .withColumn("error_saldo_origen", F.round(
        F.when(F.col("tipo") == "CASH_IN",
               F.col("saldo_ini_origen") + F.col("monto") - F.col("saldo_fin_origen"))
         .otherwise(F.col("saldo_ini_origen") - F.col("monto") - F.col("saldo_fin_origen")), 2))
    # Descuadre en el destino (los comercios 'M' no reportan saldo en PaySim)
    .withColumn("error_saldo_destino", F.when(
        F.col("tipo_destino") == "CLIENTE",
        F.round(F.col("saldo_ini_destino") + F.col("monto") - F.col("saldo_fin_destino"), 2)))
    .withColumn("vacia_cuenta", (riesgosos & (F.col("saldo_ini_origen") > 0)
                                 & (F.col("monto") >= F.col("saldo_ini_origen"))).cast("int"))
    .withColumn("_fuente", F.lit("paysim_kaggle"))
    .withColumn("_fecha_proceso", F.current_timestamp()))

(silver.repartition("fecha")
       .write.mode("overwrite").partitionBy("fecha").parquet(SILVER))

# 5) Reporte de calidad (evidencia para la entrega)
reporte = {
    "job": "01_bronze_to_silver",
    "ejecutado_utc": datetime.now(timezone.utc).isoformat(),
    "filas_bronze": total_bronze,
    "filas_rechazadas_cuarentena": n_rechazados,
    "duplicados_eliminados": n_antes - n_despues,
    "filas_silver": n_despues,
    "pct_validas": round(100.0 * n_despues / max(total_bronze, 1), 4),
}
print("[calidad]", json.dumps(reporte, indent=2))
spark.createDataFrame([reporte]).coalesce(1).write.mode("overwrite").json(CALIDAD)
spark.stop()
