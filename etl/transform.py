"""
transform.py
------------
Lee las tablas crudas de PostgreSQL, calcula KPIs y guarda los resultados
en nuevas tablas dentro del mismo schema olist.

KPIs calculados:
  - customer_kpis   : valor por cliente (revenue, pedidos, ticket medio, última compra)
  - category_kpis   : rendimiento por categoría (revenue, pedidos, precio medio,
                       puntuación media, % entregas a tiempo)

Ejecutar desde dentro del contenedor spark-master:
    docker exec -it spark_master /opt/spark/bin/spark-submit /app/etl/transform.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

JDBC_URL = "jdbc:postgresql://postgres:5432/etl_db"
JDBC_PROPS = {
    "user": "etl_user",
    "password": "etl_pass",
    "driver": "org.postgresql.Driver",
}

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("olist-transform")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Lectura desde PostgreSQL
# ---------------------------------------------------------------------------

def read_table(table):
    return spark.read.jdbc(url=JDBC_URL, table=f"olist.{table}", properties=JDBC_PROPS)

print("\n=== TRANSFORM: leyendo tablas desde PostgreSQL ===\n")

orders          = read_table("orders")
customers       = read_table("customers")
order_items     = read_table("order_items")
order_reviews   = read_table("order_reviews")
products        = read_table("products")

# ---------------------------------------------------------------------------
# KPI 1: Valor por cliente (customer_kpis)
# ---------------------------------------------------------------------------
# Usamos customer_unique_id en lugar de customer_id porque un mismo cliente
# puede tener varios customer_id en el dataset Olist (uno por pedido).
# Esto garantiza que agrupamos correctamente por persona real.
# ---------------------------------------------------------------------------

customer_kpis = (
    order_items
    .join(
        orders.select("order_id", "customer_id", "order_purchase_timestamp"),
        "order_id"
    )
    .join(
        customers.select("customer_id", "customer_unique_id", "customer_city", "customer_state"),
        "customer_id"
    )
    .groupBy("customer_unique_id", "customer_city", "customer_state")
    .agg(
        # Revenue total: precio del producto + coste de envío
        F.round(F.sum(F.col("price") + F.col("freight_value")), 2)
         .alias("total_revenue"),

        # Número de pedidos distintos
        F.countDistinct("order_id")
         .alias("total_orders"),

        # Ticket medio = revenue total / número de pedidos
        F.round(
            F.sum(F.col("price") + F.col("freight_value")) / F.countDistinct("order_id"), 2
        ).alias("avg_ticket"),

        # Fecha del último pedido
        F.max("order_purchase_timestamp")
         .alias("last_order_date"),
    )
)

# ---------------------------------------------------------------------------
# KPI 2: Rendimiento por categoría (category_kpis)
# ---------------------------------------------------------------------------
# Para el % de entregas a tiempo usamos F.avg sobre un campo 0/1:
# si el pedido llegó antes o en la fecha estimada → 1, si no → 0.
# Multiplicamos por 100 para obtener el porcentaje.
# Excluimos filas donde order_delivered_customer_date es null
# (pedidos pendientes o cancelados).
# ---------------------------------------------------------------------------

category_kpis = (
    order_items
    .join(
        products.select("product_id", "product_category_name"),
        "product_id"
    )
    .join(
        orders.select("order_id", "order_delivered_customer_date", "order_estimated_delivery_date"),
        "order_id"
    )
    .join(
        order_reviews.select("order_id", "review_score"),
        "order_id",
        "left"   # left join: incluimos pedidos sin review
    )
    .filter(F.col("product_category_name").isNotNull())
    .groupBy("product_category_name")
    .agg(
        # Revenue total de la categoría
        F.round(F.sum("price"), 2)
         .alias("total_revenue"),

        # Número de pedidos distintos
        F.countDistinct("order_id")
         .alias("total_orders"),

        # Precio medio del producto
        F.round(F.avg("price"), 2)
         .alias("avg_price"),

        # Puntuación media de reviews (ignorando nulls automáticamente)
        F.round(F.avg("review_score"), 2)
         .alias("avg_review_score"),

        # % entregas a tiempo (solo sobre pedidos ya entregados)
        F.round(
            F.avg(
                F.when(
                    F.col("order_delivered_customer_date").isNotNull() &
                    (F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date")),
                    1
                ).otherwise(0)
            ) * 100, 1
        ).alias("on_time_delivery_pct"),
    )
    .orderBy(F.col("total_revenue").desc())
)

# ---------------------------------------------------------------------------
# Escritura en PostgreSQL
# ---------------------------------------------------------------------------

def write_table(df, table):
    print(f"Guardando olist.{table}...")
    df.write.jdbc(url=JDBC_URL, table=f"olist.{table}", mode="overwrite", properties=JDBC_PROPS)
    print(f"  ✓ olist.{table} → {df.count()} filas")

write_table(customer_kpis, "customer_kpis")
write_table(category_kpis, "category_kpis")

print("\n=== TRANSFORM completado ===\n")

spark.stop()
