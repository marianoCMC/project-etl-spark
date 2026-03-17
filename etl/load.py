"""
load.py
-------
Lee las tablas de KPIs desde PostgreSQL y las exporta como ficheros Parquet
en data/processed/, que es el destino final del pipeline ETL.

Parquet es el formato estándar en pipelines de datos modernos:
  - Almacenamiento en columnas (más eficiente para queries analíticas)
  - Compresión integrada (ocupa mucho menos que CSV)
  - Compatible con Power BI, Spark, pandas, AWS S3, Azure Data Lake, etc.

Ejecutar desde dentro del contenedor spark-master:
    docker exec -it spark_master /opt/spark/bin/spark-submit /app/etl/load.py
"""

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

JDBC_URL = "jdbc:postgresql://postgres:5432/etl_db"
JDBC_PROPS = {
    "user": "etl_user",
    "password": "etl_pass",
    "driver": "org.postgresql.Driver",
}

OUTPUT_PATH = "/app/data/processed"

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("olist-load")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Lectura desde PostgreSQL y exportación a Parquet
# ---------------------------------------------------------------------------
# mode("overwrite") sobreescribe el fichero si ya existe — idempotente.
# coalesce(1) fuerza que se genere un único fichero por tabla en lugar de
# múltiples particiones, lo que facilita la lectura desde Power BI.
# ---------------------------------------------------------------------------

TABLES = ["customer_kpis", "category_kpis"]

print("\n=== LOAD: exportando KPIs a Parquet ===\n")

for table in TABLES:
    print(f"Exportando olist.{table}...")

    df = spark.read.jdbc(url=JDBC_URL, table=f"olist.{table}", properties=JDBC_PROPS)

    (
        df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(f"{OUTPUT_PATH}/{table}")
    )

    print(f"  ✓ {OUTPUT_PATH}/{table}/ → {df.count()} filas exportadas")

print("\n=== LOAD completado ===\n")

spark.stop()
