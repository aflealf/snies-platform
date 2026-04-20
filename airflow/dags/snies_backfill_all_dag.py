"""
DAG: snies_backfill_all

Carga TODOS los años disponibles en el catálogo SNIES en una sola corrida.

Útil para:
    - Primer arranque de la plataforma (carga histórica completa).
    - Recargar todos los años después de cambios en utils/snies_catalog.py.

Cómo funciona:
    Usa Dynamic Task Mapping (Airflow 2.3+) para crear N tareas paralelas,
    una por cada combinación (año × perfil) del catálogo.

    Con 3 años × 2 perfiles → 6 tareas dinámicas, limitadas a 2 concurrentes
    para no saturar al servidor del SNIES.

Ejecución:
    - UI: activar el DAG y click en 'Trigger DAG' (sin parámetros).
    - CLI: docker compose exec airflow-scheduler airflow dags trigger snies_backfill_all
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

from utils.extractors import extract_snies_data
from utils.loaders import load_to_bronze
from utils.snies_catalog import list_available_years

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "snies_dwh"
PERFILES = ["matriculados", "docentes"]

# Dataset compartido con snies_ingest. Publicar aquí también dispara snies_transform.
bronze_updated = Dataset("dwh://bronze/updated")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}


with DAG(
    dag_id="snies_backfill_all",
    description="Carga histórica completa: todos los años × todos los perfiles del catálogo.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["snies", "ingestion", "bronze", "backfill"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    # Publica el Dataset bronze_updated para disparar snies_transform.
    # trigger_rule='all_done': se ejecuta incluso si algunas tareas fallaron.
    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_done",
        outlets=[bronze_updated],
    )

    # -----------------------------------------------------------------------
    # 1. Generar la lista de combinaciones (año × perfil) a procesar
    # -----------------------------------------------------------------------
    @task(task_id="generate_workload")
    def generate_workload() -> list[dict[str, Any]]:
        """
        Genera dinámicamente la lista de tareas a ejecutar.

        Retorna:
            [{"anio": 2022, "perfil": "matriculados"}, ...]
        """
        workload = [
            {"anio": anio, "perfil": perfil}
            for anio in list_available_years()
            for perfil in PERFILES
        ]
        logger.info(f"📋 Workload: {len(workload)} tareas")
        for item in workload:
            logger.info(f"   • {item['perfil']} {item['anio']}")
        return workload

    # -----------------------------------------------------------------------
    # 2. Tarea mapeada: una instancia por cada combinación
    # -----------------------------------------------------------------------
    @task(
        task_id="ingest_one",
        max_active_tis_per_dag=2,  # Paralelismo limitado: no saturar al SNIES
    )
    def ingest_one(item: dict[str, Any], **context) -> dict:
        """
        Procesa una combinación (año, perfil): extract + load.

        Se ejecuta N veces en paralelo (una por item del workload).
        Ignora el cache local (force_redownload=False porque si ya está
        en disco es válido).
        """
        anio = int(item["anio"])
        perfil = item["perfil"]
        dag_run_id = context["dag_run"].run_id

        logger.info(f"▶️  Procesando {perfil} {anio}")

        # 1. Extract (download + parse + lineage)
        df = extract_snies_data(
            anio=anio,
            perfil=perfil,
            dag_run_id=dag_run_id,
            force_redownload=False,
        )

        # 2. Load a Bronze (idempotente)
        result = load_to_bronze(
            df=df,
            perfil=perfil,
            anio=anio,
            dag_run_id=dag_run_id,
            postgres_conn_id=POSTGRES_CONN_ID,
        )

        logger.info(
            f"✓ {perfil} {anio}: {result['rows_inserted']:,} filas en Bronze"
        )
        return result

    # -----------------------------------------------------------------------
    # 3. Resumen final
    # -----------------------------------------------------------------------
    @task(task_id="summary", trigger_rule="all_done")
    def summary(results: list[dict]) -> None:
        """
        Resumen del backfill. Se ejecuta incluso si alguna tarea falló.
        """
        logger.info("=" * 60)
        logger.info("RESUMEN DEL BACKFILL")
        logger.info("=" * 60)

        if not results:
            logger.warning("No hay resultados que resumir")
            return

        total_rows = 0
        successes = 0
        failures = 0

        for r in results:
            if r and isinstance(r, dict) and r.get("status") == "success":
                rows = r.get("rows_inserted", 0)
                total_rows += rows
                successes += 1
                logger.info(
                    f"  ✓ {r['perfil']:14s} {r['anio']}: "
                    f"{rows:>10,} filas insertadas"
                )
            else:
                failures += 1

        logger.info("-" * 60)
        logger.info(f"  Tareas exitosas: {successes}")
        logger.info(f"  Tareas fallidas: {failures}")
        logger.info(f"  TOTAL FILAS EN BRONZE: {total_rows:,}")
        logger.info("=" * 60)

    # -----------------------------------------------------------------------
    # Construcción del grafo dinámico
    # -----------------------------------------------------------------------
    workload = generate_workload()
    ingest_results = ingest_one.expand(item=workload)
    summary_task = summary(ingest_results)

    start >> workload
    ingest_results >> summary_task >> end
