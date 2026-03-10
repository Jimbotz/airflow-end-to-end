FROM apache/airflow:2.8.1

# Cambiamos temporalmente al usuario administrador (root) para instalar paquetes del sistema operativo
USER root
RUN apt-get update && apt-get install -y git

# Regresamos al usuario seguro de airflow para instalar las librerías de Python
USER airflow
RUN pip install --no-cache-dir dbt-postgres