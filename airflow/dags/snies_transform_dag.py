"""
DAG: snies_transform

Pipeline de transformación dbt: construye Silver y Gold a partir de Bronze.

Estructura (granular por capa Medallion):
    start → dbt_deps → dbt_seed → dbt_run_silver → dbt_run_gold → dbt_test → end

Cómo se activa:
    - Manual desde la UI
    - O automáticamente vía Dataset cuando snies_ingest o snies_backfill_all
      terminen exitosamente

Por qué esta estructura:
    1. dbt_deps: instala dbt_utils (idempotente, sale rápido si ya está)
    2. dbt_seed: carga ies_sue.csv en silver.ies_sue
    3. dbt_run_silver: construye vistas Silver (stg_ies, stg_matriculados_*,
       stg_docentes_*). Si falla aquí, no tiene sentido correr Gold.
    4. dbt_run_gold: construye tablas Gold (dim_ies, dim_tiempo,
       fact_capacidad_academica). El fact solo tiene sentido si Silver corrió.
    5. dbt_test: valida calidad. Corre SIEMPRE (aunque Gold falle) para ver
       qué falla y tener diagnóstico.

Consumo downstream:
    Cuando este DAG termina en verde, Gold está listo para Tableau/Metabase.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)


# =============================================================================
# Configuración
# =============================================================================

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

# Directorio del proyecto dbt dentro del contenedor (montado desde ./dbt)
DBT_PROJECT_DIR = "/opt/airflow/dbt"

# Variables de entorno que dbt necesita. Se leen desde os.environ en el
# momento de parsear el DAG, que es cuando los valores del docker-compose
# ya están disponibles en el contenedor Airflow.
#
# IMPORTANTE: estas variables se pasan al BashOperator vía `env=`. Si se
# dejaran como Jinja templates ({{ var.value.get() }}), buscarían en
# Airflow Variables (metadata) que NO es donde están estas credenciales.
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
    "DWH_HOST": os.environ.get("DWH_HOST", "dwh-postgres"),
    "DWH_PORT": os.environ.get("DWH_PORT", "5432"),
    "DWH_USER": os.environ.get("DWH_USER", "snies"),
    "DWH_PASSWORD": os.environ.get("DWH_PASSWORD", ""),
    "DWH_DB": os.environ.get("DWH_DB", "snies_dwh"),
    # PATH: necesario para que el binario dbt sea encontrado
    "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
}

# =============================================================================
# Airflow Datasets (patrón 2.4+)
# =============================================================================
# Estos Datasets conectan los DAGs entre sí. Cuando snies_ingest termina
# exitosamente, actualiza bronze_updated. Este DAG reacciona escuchándolo.
# =============================================================================

bronze_updated = Dataset("dwh://bronze/updated")
gold_ready = Dataset("dwh://gold/ready")


# =============================================================================
# Helper para construir comandos dbt consistentes
# =============================================================================

def dbt_command(subcommand: str, select: str | None = None) -> str:
    """
    Genera un comando dbt con logging claro.

    Args:
        subcommand: 'seed', 'run', 'test', 'deps'.
        select: selector de modelos (ej: 'silver.*', 'gold.*').
    """
    cmd = f"cd {DBT_PROJECT_DIR} && dbt {subcommand}"
    if select:
        cmd += f" --select {select}"
    # --no-use-colors facilita lectura en los logs de Airflow
    cmd += " --no-use-colors"
    return cmd


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="snies_transform",
    description=(
        "Ejecuta dbt para construir las capas Silver y Gold del DWH. "
        "Se dispara automáticamente cuando Bronze se actualiza."
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    # Se dispara cuando el dataset 'bronze_updated' se actualiza (DAG ingest)
    schedule=[bronze_updated],
    catchup=False,
    max_active_runs=1,
    tags=["snies", "transform", "dbt", "silver", "gold"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        outlets=[gold_ready],  # Marcar Gold como listo al terminar exitoso
    )

    # -----------------------------------------------------------------------
    # 1. dbt deps — instalar dependencias (dbt_utils)
    #    Idempotente: si ya están instaladas, es casi instantáneo.
    # -----------------------------------------------------------------------
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        env=DBT_ENV,
        append_env=True,
        doc_md="Instala las dependencias del proyecto dbt (dbt_utils).",
    )

    # -----------------------------------------------------------------------
    # 2. dbt seed — cargar ies_sue.csv a silver.ies_sue
    #    Usamos --full-refresh para asegurar que los cambios en el CSV
    #    se reflejen (dbt por defecto hace INSERT incremental en seeds).
    # -----------------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=dbt_command("seed") + " --full-refresh",
        env=DBT_ENV,
        append_env=True,
        doc_md=(
            "Carga los seeds (CSV) a la capa Silver. "
            "Incluye ies_sue.csv con las 51 sedes de universidades SUE."
        ),
    )

    # -----------------------------------------------------------------------
    # 3. dbt run silver — construye las vistas de la capa Silver
    #    Usa el selector 'silver' que coincide con el tag del path models/silver/
    # -----------------------------------------------------------------------
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=dbt_command("run", select="path:models/silver"),
        env=DBT_ENV,
        append_env=True,
        doc_md=(
            "Construye los modelos Silver (vistas):\n"
            "- stg_ies: dimensión deduplicada de IES con sector normalizado\n"
            "- stg_matriculados_bogota: matriculados filtrados y agregados\n"
            "- stg_docentes_bogota: docentes filtrados y agregados"
        ),
    )

    # -----------------------------------------------------------------------
    # 4. dbt run gold — construye el star schema
    # -----------------------------------------------------------------------
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=dbt_command("run", select="path:models/gold"),
        env=DBT_ENV,
        append_env=True,
        doc_md=(
            "Construye el star schema Gold (tablas materializadas):\n"
            "- dim_ies: dimensión con atributo es_sue\n"
            "- dim_tiempo: dimensión temporal\n"
            "- fact_capacidad_academica: métrica principal del reto"
        ),
    )

    # -----------------------------------------------------------------------
    # 5. dbt test — validar calidad
    #    trigger_rule='all_done' hace que corra aunque algún modelo falle,
    #    para que veamos el diagnóstico completo.
    # -----------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_command("test"),
        env=DBT_ENV,
        append_env=True,
        trigger_rule="all_done",
        doc_md=(
            "Ejecuta los 49 tests de calidad sobre Silver y Gold:\n"
            "- not_null, unique, relationships\n"
            "- accepted_values para enumeraciones\n"
            "- unique_combination_of_columns\n"
            "- Tests custom (fact_solo_bogota)"
        ),
    )

    # -----------------------------------------------------------------------
    # Construcción del grafo
    # -----------------------------------------------------------------------
    start >> dbt_deps >> dbt_seed >> dbt_run_silver >> dbt_run_gold >> dbt_test >> end
