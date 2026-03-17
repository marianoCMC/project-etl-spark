"""
extract.py
----------
Lee los CSVs del dataset Olist desde data/raw/ y los carga en PostgreSQL.

Este script es el punto de entrada del pipeline ETL: toma los datos crudos
y los persiste en la base de datos, que será la fuente para las siguientes fases.

Ejecutar desde dentro del contenedor spark-master:
    docker exec -it spark_master spark-submit /app/etl/extract.py
"""

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

POSTGRES_URL = "jdbc:postgresql://postgres:5432/etl_db"

POSTGRES_PROPERTIES = {
    "user": "etl_user",
    "password": "etl_pass",
    "driver": "org.postgresql.Driver",
}

RAW_DATA_PATH = "/app/data/raw"


# ---------------------------------------------------------------------------
# Inicializar SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("olist-extract")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")  # reducir verbosidad de logs


# ---------------------------------------------------------------------------
# Funciones auxiliares
# ---------------------------------------------------------------------------

def read_csv(filename: str):
    """Lee un CSV desde data/raw/ y devuelve un DataFrame de Spark."""
    path = f"{RAW_DATA_PATH}/{filename}"
    return (
        spark.read
        .option("header", "true")   # primera fila = nombres de columna
        .option("inferSchema", "true")  # detecta tipos automáticamente (int, float, timestamp...)
        .csv(path)
    )


def write_to_postgres(df, table: str):
    """Escribe un DataFrame en una tabla de PostgreSQL."""
    print(f"  Cargando tabla olist.{table}...")
    (
        df.write
        .jdbc(
            url=POSTGRES_URL,
            table=f"olist.{table}",
            mode="overwrite",   # sobreescribe si ya hay datos (idempotente)
            properties=POSTGRES_PROPERTIES,
        )
    )
    print(f"  ✓ olist.{table} → {df.count()} filas cargadas")


# ---------------------------------------------------------------------------
# Pipeline de carga
# ---------------------------------------------------------------------------

print("\n=== EXTRACT: cargando CSVs en PostgreSQL ===\n")

# orders — todas las columnas coinciden con la tabla
orders = read_csv("olist_orders_dataset.csv")
write_to_postgres(orders, "orders")

# customers — todas las columnas coinciden
customers = read_csv("olist_customers_dataset.csv")
write_to_postgres(customers, "customers")

# products — el CSV tiene columnas extra que no están en la tabla
# (product_name_lenght, product_description_lenght, product_photos_qty)
products = read_csv("olist_products_dataset.csv").select(
    "product_id",
    "product_category_name",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
)
write_to_postgres(products, "products")

# order_items — todas las columnas coinciden
order_items = read_csv("olist_order_items_dataset.csv")
write_to_postgres(order_items, "order_items")

# order_payments — todas las columnas coinciden
order_payments = read_csv("olist_order_payments_dataset.csv")
write_to_postgres(order_payments, "order_payments")

# order_reviews — el CSV tiene columnas extra (review_comment_title, review_comment_message)
order_reviews = read_csv("olist_order_reviews_dataset.csv").select(
    "review_id",
    "order_id",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp",
)
write_to_postgres(order_reviews, "order_reviews")

print("\n=== EXTRACT completado ===\n")

spark.stop()
