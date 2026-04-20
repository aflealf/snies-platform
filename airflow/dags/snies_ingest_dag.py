"""
DAG: snies_ingest

Pipeline de ingesta del SNIES a la capa Bronze del DWH.

Características:
    - Parametrizado por año (default: 2024).
    - Procesa matriculados y docentes en paralelo (TaskGroups).
    - Idempotente: re-ejecutar reemplaza, no acumula.
    - Logging completo en bronze._ingestion_log.
    - Validación post-carga.

Uso desde la UI:
    1. Entrar a Airflow (http://localhost:8080).
    2. Activar el DAG 'snies_ingest'.
    3. Click en "Trigger DAG w/ config" y pasar:
       {"anio": 2024}

Uso desde CLI:
    docker compose exec airflow-scheduler \\
        airflow dags trigger snies_ingest --conf '{"anio": 2024}'

Schedule:
    Manual (None). El SNIES publica anualmente, no tiene sentido un cron.
    Para automatizar: cambiar a '@yearly' o usar Airflow Datasets.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG, Dataset
from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.extractors import extract_snies_data
from utils.loaders import load_to_bronze
from utils.snies_catalog import list_available_years

logger = logging.getLogger(__name__)


# =============================================================================
# Configuración del DAG
# =============================================================================

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}

POSTGRES_CONN_ID = "snies_dwh"
PERFILES = ["matriculados", "docentes"]
PICKLE_DIR = Path("/tmp/snies_downloads")

# Dataset que este DAG publica al terminar exitosamente.
# Dispara automáticamente el DAG snies_transform (schedule=[bronze_updated]).
bronze_updated = Dataset("dwh://bronze/updated")


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="snies_ingest",
    description="Descarga datos del SNIES y los carga a la capa Bronze del DWH.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["snies", "ingestion", "bronze"],
    params={
        "anio": Param(
            default=2024,
            type="integer",
            minimum=2022,
            maximum=2030,
            title="Año a ingerir",
            description=(
                f"Año del dataset SNIES a descargar. "
                f"Disponibles en el catálogo: {list_available_years()}"
            ),
        ),
        "force_redownload": Param(
            default=False,
            type="boolean",
            title="Forzar re-descarga",
            description="Si está activo, ignora el cache local y descarga el archivo nuevamente.",
        ),
    },
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    # 'end' publica el Dataset bronze_updated cuando se ejecuta.
    # Esto dispara automáticamente el DAG snies_transform.
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed",
        outlets=[bronze_updated],
    )

    # -----------------------------------------------------------------------
    # Validación previa: ¿el año pedido existe en el catálogo?
    # -----------------------------------------------------------------------
    @task(task_id="validate_year")
    def validate_year(**context) -> int:
        """
        Verifica que el año solicitado esté en el catálogo.
        Retorna el año como int para downstream.
        """
        # Los params siempre llegan con el tipo declarado en Param(type=...).
        # Pero por si acaso, casteamos explícitamente.
        anio = int(context["params"]["anio"])
        available = list_available_years()
        if anio not in available:
            raise ValueError(
                f"Año {anio} no está en el catálogo. "
                f"Disponibles: {available}. "
                f"→ Añade la entrada en utils/snies_catalog.py SNIES_CATALOG."
            )
        logger.info(f"✓ Año {anio} validado")
        return anio

    # -----------------------------------------------------------------------
    # Factory de TaskGroups por perfil
    # -----------------------------------------------------------------------
    def make_perfil_taskgroup(perfil: str):
        """Crea un TaskGroup con extract + load para un perfil dado."""

        @task_group(group_id=f"ingest_{perfil}")
        def perfil_group(anio: int):

            @task(task_id="extract")
            def extract(anio: int, **context) -> dict:
                """
                Descarga + parsea + enriquece con linaje.

                Materializa el DataFrame en disco (pickle) porque XCom
                usa la metadata DB de Airflow, no es para datos grandes.
                """
                anio_int = int(anio)  # defensivo: asegurar int
                dag_run_id = context["dag_run"].run_id
                force = context["params"].get("force_redownload", False)

                df = extract_snies_data(
                    anio=anio_int,
                    perfil=perfil,
                    dag_run_id=dag_run_id,
                    force_redownload=force,
                )

                PICKLE_DIR.mkdir(parents=True, exist_ok=True)
                tmp_path = PICKLE_DIR / f"_df_{perfil}_{anio_int}.pkl"
                df.to_pickle(tmp_path)

                logger.info(
                    f"DataFrame serializado en {tmp_path} "
                    f"({len(df):,} filas, {tmp_path.stat().st_size/1024/1024:.1f} MB)"
                )

                return {
                    "perfil": perfil,
                    "anio": anio_int,
                    "rows": len(df),
                    "df_path": str(tmp_path),
                }

            @task(task_id="load")
            def load(extract_meta: dict, **context) -> dict:
                """Lee el DataFrame del disco y lo carga a Bronze."""
                import pandas as pd

                dag_run_id = context["dag_run"].run_id
                df_path = Path(extract_meta["df_path"])

                df = pd.read_pickle(df_path)
                logger.info(f"DataFrame leído de {df_path} ({len(df):,} filas)")

                result = load_to_bronze(
                    df=df,
                    perfil=extract_meta["perfil"],
                    anio=extract_meta["anio"],
                    dag_run_id=dag_run_id,
                    postgres_conn_id=POSTGRES_CONN_ID,
                )

                df_path.unlink(missing_ok=True)
                return result

            extract_meta = extract(anio=anio)
            load(extract_meta)

        return perfil_group

    # -----------------------------------------------------------------------
    # Validación post-carga: contar filas Bogotá vs total
    # -----------------------------------------------------------------------
    @task(task_id="validate_load")
    def validate_load(anio: int, **context) -> dict:
        """
        Verifica que la carga produjo datos en Bronze.
        """
        anio_int = int(anio)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        results = {}
        for perfil in PERFILES:
            table = f"bronze.{perfil}_raw"

            count_total = hook.get_first(
                f"SELECT COUNT(*) FROM {table} WHERE anio = %s",
                parameters=(str(anio_int),),
            )[0]

            # El SNIES usa "Bogotá D.C." (2022) y "Bogotá, D.C." (2023+).
            # LIKE 'BOGOT%' cubre ambos, UPPER+tilde-friendly.
            count_bogota = hook.get_first(
                f"""
                SELECT COUNT(*) FROM {table}
                WHERE anio = %s
                  AND UPPER(departamento_domicilio_ies) LIKE 'BOGOT%%'
                """,
                parameters=(str(anio_int),),
            )[0]

            results[perfil] = {"total": count_total, "bogota": count_bogota}

            logger.info(
                f"  {perfil} {anio_int}: {count_total:,} totales | "
                f"{count_bogota:,} en Bogotá"
            )

            if count_total == 0:
                raise ValueError(
                    f"Validación FALLÓ: no se cargaron filas de {perfil} para {anio_int}. "
                    f"Revisa logs de la tarea ingest_{perfil}.load."
                )

            if count_bogota == 0:
                logger.warning(
                    f"⚠️  {perfil} {anio_int}: 0 filas de Bogotá. "
                    f"¿Cambió el nombre del departamento? Revisar en Bronze."
                )

        logger.info("✅ Validación post-carga OK")
        return results

    # -----------------------------------------------------------------------
    # Construcción del grafo usando XComArgs (TaskFlow API)
    # Esto preserva el tipo del año como int a lo largo del pipeline.
    # -----------------------------------------------------------------------
    year_validated = validate_year()

    matriculados_tg = make_perfil_taskgroup("matriculados")(anio=year_validated)
    docentes_tg = make_perfil_taskgroup("docentes")(anio=year_validated)

    validation = validate_load(anio=year_validated)

    start >> year_validated
    [matriculados_tg, docentes_tg] >> validation
    validation >> end
