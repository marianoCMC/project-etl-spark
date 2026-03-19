"""
olist_pipeline.py
-----------------
DAG de Airflow que orquesta el pipeline ETL + ML completo del dataset Olist.

Flujo de tareas:
    extract → transform → load → train → predict

Cada tarea ejecuta un spark-submit dentro del contenedor spark_master
usando docker exec. Las dependencias (>>) garantizan el orden de ejecución.

Programación: diaria a las 6:00 AM (schedule='0 6 * * *')
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

# Comando base: ejecuta spark-submit dentro del contenedor spark_master
SPARK_SUBMIT = "docker exec spark_master /opt/spark/bin/spark-submit"

default_args = {
    "owner": "airflow",
    "retries": 1,                           # reintenta 1 vez si una tarea falla
    "retry_delay": timedelta(minutes=5),    # espera 5 min antes de reintentar
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
# catchup=False → no ejecuta las fechas pasadas desde start_date
# schedule='0 6 * * *' → todos los días a las 6:00 AM (formato cron)
# ---------------------------------------------------------------------------

with DAG(
    dag_id="olist_pipeline",
    description="Pipeline ETL + ML del dataset Olist",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["olist", "etl", "ml"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 — Extract
    # Lee los CSVs y los carga en PostgreSQL
    # -----------------------------------------------------------------------
    extract = BashOperator(
        task_id="extract",
        bash_command=f"{SPARK_SUBMIT} /app/etl/extract.py",
    )

    # -----------------------------------------------------------------------
    # Task 2 — Transform
    # Calcula KPIs de cliente y categoría
    # -----------------------------------------------------------------------
    transform = BashOperator(
        task_id="transform",
        bash_command=f"{SPARK_SUBMIT} /app/etl/transform.py",
    )

    # -----------------------------------------------------------------------
    # Task 3 — Load
    # Exporta los KPIs a Parquet
    # -----------------------------------------------------------------------
    load = BashOperator(
        task_id="load",
        bash_command=f"{SPARK_SUBMIT} /app/etl/load.py",
    )

    # -----------------------------------------------------------------------
    # Task 4 — Train
    # Re-entrena el modelo K-Means con los datos actualizados
    # -----------------------------------------------------------------------
    train = BashOperator(
        task_id="train",
        bash_command=f"{SPARK_SUBMIT} /app/ml/train.py",
    )

    # -----------------------------------------------------------------------
    # Task 5 — Predict
    # Clasifica los clientes nuevos con el modelo recién entrenado
    # -----------------------------------------------------------------------
    predict = BashOperator(
        task_id="predict",
        bash_command=f"{SPARK_SUBMIT} /app/ml/predict.py",
    )

    # -----------------------------------------------------------------------
    # Dependencias — definen el orden de ejecución
    # -----------------------------------------------------------------------
    # >> significa "debe ejecutarse antes que"
    # Si una tarea falla, las siguientes no se ejecutan

    extract >> transform >> load >> train >> predict
