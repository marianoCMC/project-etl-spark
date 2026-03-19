"""
predict.py
----------
Carga el modelo K-Means entrenado y lo aplica a nuevos clientes.

En producción este script recibiría clientes que llegaron después del
entrenamiento. Aquí lo simulamos leyendo customer_kpis y filtrando
los clientes que aún no tienen segmento asignado en customer_segments.

Ejecutar desde dentro del contenedor spark-master:
    docker exec -it spark_master /opt/spark/bin/spark-submit /app/ml/predict.py
"""

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

JDBC_URL = "jdbc:postgresql://postgres:5432/etl_db"
JDBC_PROPS = {
    "user": "etl_user",
    "password": "etl_pass",
    "driver": "org.postgresql.Driver",
}

MODEL_PATH  = "/app/ml/models/kmeans_customer_segmentation"
OUTPUT_PATH = "/app/data/processed/customer_segments_new"

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("olist-kmeans-predict")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Cargar modelo entrenado
# ---------------------------------------------------------------------------
# PipelineModel.load() reconstruye el pipeline completo:
# VectorAssembler + StandardScaler (con las medias/std del entrenamiento)
# + KMeans (con los centroides aprendidos).
# Esto garantiza que los nuevos datos se escalan igual que los de entrenamiento.
# ---------------------------------------------------------------------------

print("\n=== PREDICT: aplicando modelo a nuevos clientes ===\n")

model = PipelineModel.load(MODEL_PATH)
print("Modelo cargado correctamente")

# ---------------------------------------------------------------------------
# Nuevos clientes — clientes sin segmento asignado
# ---------------------------------------------------------------------------
# Simulamos "nuevos clientes" como aquellos que están en customer_kpis
# pero no tienen todavía registro en customer_segments.
# En un entorno real, estos vendrían de un proceso de ingesta incremental.
# ---------------------------------------------------------------------------

all_customers = (
    spark.read
    .jdbc(url=JDBC_URL, table="olist.customer_kpis", properties=JDBC_PROPS)
    .select("customer_unique_id", "customer_city", "customer_state",
            "total_revenue", "total_orders", "avg_ticket")
    .dropna()
)

segmented_customers = (
    spark.read
    .jdbc(url=JDBC_URL, table="olist.customer_segments", properties=JDBC_PROPS)
    .select("customer_unique_id")
)

# Anti-join: clientes que NO están en customer_segments
new_customers = all_customers.join(
    segmented_customers,
    on="customer_unique_id",
    how="left_anti",
)

count = new_customers.count()
print(f"Nuevos clientes a clasificar: {count}")

if count == 0:
    print("No hay clientes nuevos. Vuelve a ejecutar train.py con datos actualizados.")
    spark.stop()
    exit(0)

# ---------------------------------------------------------------------------
# Predicción
# ---------------------------------------------------------------------------

predictions = model.transform(new_customers)

output = predictions.select(
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "total_revenue",
    "total_orders",
    "avg_ticket",
    "segment",
)

# ---------------------------------------------------------------------------
# Guardar resultados
# ---------------------------------------------------------------------------

# Añadir a la tabla existente en PostgreSQL (append, no overwrite)
print("Guardando nuevos segmentos en PostgreSQL...")
output.write.jdbc(
    url=JDBC_URL,
    table="olist.customer_segments",
    mode="append",
    properties=JDBC_PROPS,
)

# Parquet
print("Guardando en Parquet...")
output.coalesce(1).write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"\n  ✓ {count} nuevos clientes clasificados y guardados")
print("\n=== PREDICT completado ===\n")

spark.stop()
