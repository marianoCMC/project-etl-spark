FROM bitnami/spark:3.5

USER root

# Instalar dependencias Python
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    scikit-learn \
    pyspark==3.5.0

# Driver JDBC de PostgreSQL (necesario para que Spark lea desde PostgreSQL)
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /opt/bitnami/spark/jars/

WORKDIR /app
