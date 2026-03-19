FROM apache/airflow:2.9.0

USER root

# Docker CLI — necesario para que BashOperator pueda ejecutar
# "docker exec spark_master spark-submit ..." desde dentro de Airflow
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    usermod -aG docker airflow

USER airflow
