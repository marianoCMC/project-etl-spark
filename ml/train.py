"""
train.py
--------
Entrena un modelo de segmentación de clientes usando K-Means (MLlib).

Pipeline:
    customer_kpis (PostgreSQL)
        → VectorAssembler   — une las features en un vector
        → StandardScaler    — normaliza las escalas
        → KMeans (k=4)      — asigna cada cliente a un segmento

El resultado se guarda en:
    - PostgreSQL: olist.customer_segments
    - Parquet:    data/processed/customer_segments/

Ejecutar desde dentro del contenedor spark-master:
    docker exec -it spark_master /opt/spark/bin/spark-submit /app/ml/train.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

JDBC_URL = "jdbc:postgresql://postgres:5432/etl_db"
JDBC_PROPS = {
    "user": "etl_user",
    "password": "etl_pass",
    "driver": "org.postgresql.Driver",
}

OUTPUT_PATH = "/app/data/processed/customer_segments"
MODEL_PATH = "/app/ml/models/kmeans_customer_segmentation"
K = 4    # número de segmentos
SEED = 42

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("olist-kmeans")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

print("\n=== ML: entrenando segmentación de clientes ===\n")

customer_kpis = (
    spark.read
    .jdbc(url=JDBC_URL, table="olist.customer_kpis", properties=JDBC_PROPS)
    .select("customer_unique_id", "customer_city", "customer_state",
            "total_revenue", "total_orders", "avg_ticket")
    .dropna()    # eliminar filas con nulls antes de entrenar
)

print(f"Clientes cargados: {customer_kpis.count()}")

# ---------------------------------------------------------------------------
# Pipeline MLlib
# ---------------------------------------------------------------------------

assembler = VectorAssembler(
    inputCols=["total_revenue", "total_orders", "avg_ticket"],
    outputCol="features_raw",
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,   # centra en media 0
    withStd=True,    # escala a desviación estándar 1
)

kmeans = KMeans(
    featuresCol="features",
    predictionCol="segment",
    k=K,
    seed=SEED,
)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])

# ---------------------------------------------------------------------------
# Entrenamiento
# ---------------------------------------------------------------------------
# fit() aprende: medias/std del scaler + posición de centroides
# transform() aplica el modelo y añade la columna "segment"
# ---------------------------------------------------------------------------

model = pipeline.fit(customer_kpis)
result = model.transform(customer_kpis)

# ---------------------------------------------------------------------------
# Evaluación — Silhouette score
# ---------------------------------------------------------------------------
# El Silhouette score mide la calidad del clustering:
#   +1.0 → segmentos muy compactos y bien separados
#    0.0 → segmentos solapados
#   -1.0 → clientes asignados al segmento equivocado
# Un valor > 0.5 se considera aceptable.
# ---------------------------------------------------------------------------

evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="segment",
    metricName="silhouette",
)

silhouette = evaluator.evaluate(result)
print(f"Silhouette score: {silhouette:.4f}")

# ---------------------------------------------------------------------------
# Resumen de segmentos
# ---------------------------------------------------------------------------

print("\nResumen por segmento:")
(
    result
    .groupBy("segment")
    .agg(
        F.count("customer_unique_id").alias("num_clientes"),
        F.round(F.avg("total_revenue"), 2).alias("avg_revenue"),
        F.round(F.avg("total_orders"), 2).alias("avg_orders"),
        F.round(F.avg("avg_ticket"), 2).alias("avg_ticket"),
    )
    .orderBy("segment")
    .show()
)

# ---------------------------------------------------------------------------
# Guardar resultados
# ---------------------------------------------------------------------------

output = result.select(
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "total_revenue",
    "total_orders",
    "avg_ticket",
    "segment",
)

# PostgreSQL
print("Guardando olist.customer_segments en PostgreSQL...")
output.write.jdbc(
    url=JDBC_URL,
    table="olist.customer_segments",
    mode="overwrite",
    properties=JDBC_PROPS,
)

# Parquet
print("Guardando customer_segments en Parquet...")
output.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)

# Modelo entrenado (necesario para predict.py)
print("Guardando modelo en disco...")
model.write().overwrite().save(MODEL_PATH)

print(f"\n  ✓ {output.count()} clientes segmentados y guardados")
print("\n=== ML completado ===\n")

spark.stop()
