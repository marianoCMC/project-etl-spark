FROM apache/spark-py:v3.4.0

USER root

# Instalar dependencias Python
# pyspark no se instala aquí porque apache/spark-py ya lo incluye en la versión correcta
RUN pip install --no-cache-dir \
    psycopg2-binary \
    pandas \
    scikit-learn

# Driver JDBC de PostgreSQL (necesario para que Spark lea/escriba en PostgreSQL)
ADD https://jdbc.postgresql.org/download/postgresql-42.6.0.jar /opt/spark/jars/

WORKDIR /app
