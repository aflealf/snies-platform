# ============================================================================
# Airflow + dbt custom image
# ============================================================================
# Base: imagen oficial de Airflow 2.9 con Python 3.11.
# Añadimos dbt-postgres y las librerías necesarias para extraer .xlsx del SNIES.
# ============================================================================

FROM apache/airflow:2.9.3-python3.11

# Cambiamos a root solo para instalar paquetes del sistema si hiciera falta.
USER root

# Aseguramos que el usuario airflow pueda escribir en /opt/airflow/dbt
RUN mkdir -p /opt/airflow/dbt && \
    chown -R airflow:0 /opt/airflow/dbt

# Volvemos a usuario airflow para instalar libs Python (best practice oficial)
USER airflow

# Copiamos e instalamos requirements
COPY --chown=airflow:0 requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Configuramos DBT_PROFILES_DIR para que dbt encuentre profiles.yml
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
