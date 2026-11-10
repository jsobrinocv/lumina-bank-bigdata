"""
Lumina Bank - Job Spark 2: SILVER -> GOLD (modelo estrella)
Construye las dimensiones y la tabla de hechos, mas la tabla de patrones de
gasto que consume el pipeline de tiempo real.

  gold/fact_transaccion        hechos (1 fila = 1 transaccion)
  gold/dim_cliente             clientes (sintetico, deterministico por hash)
  gold/dim_distrito            8 distritos financieros de DataLandia
  gold/dim_fecha               calendario + dias de pago + eventos/feriados
  gold/dim_tipo_transaccion    catalogo de productos y comisiones (supuestos)
  gold/patron_gasto            patron historico por tipo x segmento (para streaming)

Ejecucion:
  gcloud dataproc jobs submit pyspark gs://$BUCKET/scripts/02_silver_to_gold.py \
     --cluster=$CLUSTER --region=$REGION -- --bucket=$BUCKET
"""
import argparse
from pyspark.sql import SparkSession, functions as F

ap = argparse.ArgumentParser()
ap.add_argument("--bucket", required=True)
args = ap.parse_args()

SILVER = f"gs://{args.bucket}/silver/transacciones/"
EVENTOS = f"gs://{args.bucket}/bronze/eventos/"
GOLD = f"gs://{args.bucket}/gold"

spark = (SparkSession.builder.appName("lumina_silver_to_gold")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
         .getOrCreate())

CANALES = ["APP", "CAJERO", "OFICINA", "AGENTE"]


def h(col, inicio, largo=8):
    """Entero deterministico a partir del MD5 de un texto.
    Es la MISMA formula que usa el pipeline de streaming (Python hashlib),
    asi batch y tiempo real asignan el mismo distrito/segmento a cada cliente."""
    return F.conv(F.substring(F.md5(col), inicio, largo), 16, 10).cast("long")


def atributos_cliente(df, col_id):
    """Agrega distrito, segmento etario y canal preferido (tabla de clientes sintetica)."""
    u_edad = h(F.col(col_id), 9) % 100
    u_canal = h(F.col(col_id), 17) % 100
    segmento = (F.when(u_edad < 20, "18-25").when(u_edad < 55, "26-40")
                 .when(u_edad < 85, "41-60").otherwise("60+"))
    df = df.withColumn("distrito_id", (h(F.col(col_id), 1) % 8 + 1).cast("int")) \
           .withColumn("segmento_etario", segmento)
    canal = (F.when(F.col("segmento_etario") == "60+",
                    F.when(u_canal < 45, "OFICINA").when(u_canal < 75, "CAJERO")
                     .when(u_canal < 90, "AGENTE").otherwise("APP"))
              .when(F.col("segmento_etario") == "41-60",
                    F.when(u_canal < 25, "OFICINA").when(u_canal < 45, "CAJERO")
                     .when(u_canal < 55, "AGENTE").otherwise("APP"))
              .otherwise(F.when(u_canal < 5, "OFICINA").when(u_canal < 15, "CAJERO")
                          .when(u_canal < 25, "AGENTE").otherwise("APP")))
    return df.withColumn("canal_preferido", canal)


silver = spark.read.parquet(SILVER)

# ---------------- dim_distrito ----------------
distritos = [
    (1, "Distrito Central", "COMERCIAL", "lima"),
    (2, "Puerto Norte", "MIXTO", "callao"),
    (3, "Valle Sur", "RESIDENCIAL", "arequipa"),
    (4, "Las Colinas", "RESIDENCIAL", "libertad"),
    (5, "Ribera Este", "MIXTO", "piura"),
    (6, "Nuevo Horizonte", "COMERCIAL", "lambayeque"),
    (7, "San Data", "RESIDENCIAL", "cusco|cuzco"),
    (8, "Altos del Oeste", "MIXTO", "jun[ií]n"),
]
dim_distrito = spark.createDataFrame(
    distritos, "distrito_id INT, distrito_nombre STRING, tipo_zona STRING, region_trends_patron STRING")

# ---------------- dim_tipo_transaccion (comisiones = SUPUESTOS de negocio) ----------------
tipos = [
    ("CASH_IN", "Deposito en efectivo", "Depositos", "ENTRADA", 1, 0.000),
    ("CASH_OUT", "Retiro de efectivo", "Retiros", "SALIDA", 1, 0.005),
    ("DEBIT", "Debito automatico", "Debitos", "SALIDA", 0, 0.002),
    ("PAYMENT", "Pago a comercio", "Pagos", "SALIDA", 0, 0.015),
    ("TRANSFER", "Transferencia", "Transferencias", "SALIDA", 0, 0.003),
]
dim_tipo = spark.createDataFrame(
    tipos, "tipo STRING, descripcion STRING, producto STRING, sentido STRING, "
           "afecta_efectivo INT, comision_pct DOUBLE")

# ---------------- dim_cliente (~9 M clientes: origenes + destinos tipo C) ----------------
ids = (silver.select(F.col("cliente_origen_id").alias("cliente_id"))
       .union(silver.filter(F.col("cliente_destino_id").startswith("C"))
                    .select(F.col("cliente_destino_id").alias("cliente_id")))
       .distinct())
dim_cliente = atributos_cliente(ids, "cliente_id")

# ---------------- dim_fecha ----------------
eventos = (spark.read.option("header", True).csv(EVENTOS)
           .select(F.to_date("fecha").alias("fecha"), "evento", "tipo_evento",
                   F.col("es_feriado").cast("int").alias("es_feriado")))
rango = silver.agg(F.min("fecha").alias("ini"), F.max("fecha").alias("fin"))
dias = ["domingo", "lunes", "martes", "miercoles", "jueves", "viernes", "sabado"]
dim_fecha = (rango.select(F.explode(F.sequence("ini", F.date_add("fin", 7))).alias("fecha"))
    .withColumn("anio", F.year("fecha"))
    .withColumn("mes", F.month("fecha"))
    .withColumn("dia", F.dayofmonth("fecha"))
    .withColumn("dia_semana", F.dayofweek("fecha"))
    .withColumn("nombre_dia", F.element_at(F.array(*[F.lit(d) for d in dias]), F.dayofweek("fecha")))
    .withColumn("es_fin_semana", F.dayofweek("fecha").isin(1, 7).cast("int"))
    .withColumn("es_dia_pago", ((F.dayofmonth("fecha") == 15)
                                | (F.col("fecha") == F.last_day("fecha"))).cast("int"))
    .withColumn("es_cierre_mes", (F.datediff(F.last_day("fecha"), F.col("fecha")) <= 1).cast("int"))
    .join(eventos, "fecha", "left")
    .withColumn("es_feriado", F.coalesce("es_feriado", F.lit(0))))

# ---------------- fact_transaccion ----------------
fact = atributos_cliente(silver, "cliente_origen_id")
u_tx = h(F.col("transaccion_id"), 1) % 100
fact = fact.withColumn(
    "canal",
    F.when(u_tx < 75, F.col("canal_preferido"))
     .otherwise(F.element_at(F.array(*[F.lit(c) for c in CANALES]), (u_tx % 4 + 1).cast("int"))))

fact = fact.select(
    "transaccion_id", "fecha", "hora", "ts_evento", "step", "tipo",
    "cliente_origen_id", "cliente_destino_id", "tipo_destino", "distrito_id", "canal",
    "monto", "saldo_ini_origen", "saldo_fin_origen", "saldo_ini_destino", "saldo_fin_destino",
    "error_saldo_origen", "error_saldo_destino", "vacia_cuenta",
    "es_fraude", "alerta_sistema_legado")

# ---------------- Escritura GOLD (Parquet sin particion de carpeta -> bq load directo) ----------------
salidas = {
    "fact_transaccion": fact.repartition(16),
    "dim_cliente": dim_cliente.repartition(8),
    "dim_fecha": dim_fecha.coalesce(1),
    "dim_distrito": dim_distrito.coalesce(1),
    "dim_tipo_transaccion": dim_tipo.coalesce(1),
}
for nombre, d in salidas.items():
    d.write.mode("overwrite").parquet(f"{GOLD}/{nombre}/")
    print(f"[gold] {nombre}: escrito")

# ---------------- Patron de gasto historico ("patrones aprendidos") -> lo usa el streaming ----------------
# Se calcula DESPUES de escribir los hechos, leyendo el Parquet ya generado:
# asi Spark no recalcula toda la cadena silver -> fact (ahorra memoria en el nodo unico).
# El segmento etario se obtiene con la misma funcion hash (sin join con dim_cliente).
fact_gold = atributos_cliente(
    spark.read.parquet(f"{GOLD}/fact_transaccion/").select("cliente_origen_id", "tipo", "monto", "es_fraude"),
    "cliente_origen_id")
patron = (fact_gold.groupBy("tipo", "segmento_etario").agg(
    F.count("*").alias("n_transacciones"),
    F.round(F.avg("monto"), 2).alias("monto_promedio"),
    F.round(F.stddev("monto"), 2).alias("monto_desv"),
    F.round(F.percentile_approx("monto", 0.95, 1000), 2).alias("monto_p95"),
    F.round(F.avg("es_fraude"), 6).alias("tasa_fraude")))
patron.repartition(1).write.mode("overwrite").parquet(f"{GOLD}/patron_gasto/")
print("[gold] patron_gasto: escrito")

print("[gold] filas fact:", spark.read.parquet(f"{GOLD}/fact_transaccion/").count())
print("[gold] filas dim_cliente:", spark.read.parquet(f"{GOLD}/dim_cliente/").count())
spark.stop()
