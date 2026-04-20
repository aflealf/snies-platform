"""
Loader: carga DataFrames a la capa Bronze del DWH.

Estrategia de carga:
    1. Crea la tabla Bronze si no existe (schema-on-read: todo TEXT)
    2. Elimina filas del año/perfil que se está recargando (idempotencia)
    3. Inserta el nuevo DataFrame
    4. Registra la ejecución en bronze._ingestion_log

Por qué TRUNCATE-por-partición en lugar de MERGE:
    - Los Excel del SNIES son snapshots completos por año.
    - Un reload debe reemplazar, no acumular.
    - TRUNCATE WHERE _source_file = X es más rápido y más simple que MERGE
      sobre 300K filas con hash matching.
"""

from __future__ import annotations

import logging
from typing import Literal

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

logger = logging.getLogger(__name__)

Perfil = Literal["matriculados", "docentes"]

BRONZE_SCHEMA = "bronze"

# Mapeo perfil → tabla Bronze
BRONZE_TABLES: dict[Perfil, str] = {
    "matriculados": "matriculados_raw",
    "docentes": "docentes_raw",
}


# =============================================================================
# Gestión de la tabla Bronze (schema-on-read)
# =============================================================================

def ensure_bronze_table_exists(
    hook: PostgresHook,
    perfil: Perfil,
    df: pd.DataFrame,
) -> None:
    """
    Crea la tabla Bronze si no existe, con las columnas que trae el DataFrame.

    Todas las columnas de datos son TEXT (schema-on-read).
    Las columnas de linaje tienen tipos específicos.

    Idempotente: si la tabla ya existe, añade columnas nuevas que falten
    (útil cuando el SNIES añade columnas en años posteriores).
    """
    table = BRONZE_TABLES[perfil]
    fqtn = f'"{BRONZE_SCHEMA}"."{table}"'

    # Columnas de linaje con tipos específicos
    lineage_columns = {
        "_ingested_at": "TIMESTAMP",
        "_source_url": "TEXT",
        "_source_file": "TEXT",
        "_row_hash": "TEXT",
        "_dag_run_id": "TEXT",
    }

    # Columnas de datos: todas TEXT
    data_columns = [c for c in df.columns if c not in lineage_columns]

    # --- 1. Crear tabla si no existe ---
    columns_sql = [f'"{col}" TEXT' for col in data_columns]
    columns_sql += [f'"{col}" {dtype}' for col, dtype in lineage_columns.items()]
    columns_sql_str = ",\n    ".join(columns_sql)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {fqtn} (
        {columns_sql_str}
    );
    """

    # Índices útiles para queries de Silver
    indexes_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_{table}_anio
        ON {fqtn} ("anio");
    CREATE INDEX IF NOT EXISTS idx_{table}_source_file
        ON {fqtn} ("_source_file");
    CREATE INDEX IF NOT EXISTS idx_{table}_row_hash
        ON {fqtn} ("_row_hash");
    """

    hook.run(create_sql)
    hook.run(indexes_sql)
    logger.info(f"✓ Tabla {fqtn} asegurada ({len(data_columns)} columnas de datos)")

    # --- 2. Añadir columnas nuevas si el schema cambió ---
    existing_cols = _get_existing_columns(hook, BRONZE_SCHEMA, table)
    missing_cols = [c for c in data_columns if c not in existing_cols]
    for col in missing_cols:
        alter_sql = f'ALTER TABLE {fqtn} ADD COLUMN IF NOT EXISTS "{col}" TEXT;'
        hook.run(alter_sql)
        logger.info(f"  + Columna añadida: {col}")


def _get_existing_columns(hook: PostgresHook, schema: str, table: str) -> set[str]:
    """Retorna el set de columnas existentes en la tabla."""
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
    """
    records = hook.get_records(sql, parameters=(schema, table))
    return {r[0] for r in records}


# =============================================================================
# Carga idempotente
# =============================================================================

def delete_existing_partition(
    hook: PostgresHook,
    perfil: Perfil,
    source_file: str,
) -> int:
    """
    Elimina filas previamente cargadas desde el mismo source_file.

    Esto hace la ingesta idempotente: re-ejecutar el DAG con el mismo año
    reemplaza (no duplica) los datos.

    Returns:
        Número de filas eliminadas.
    """
    table = BRONZE_TABLES[perfil]
    fqtn = f'"{BRONZE_SCHEMA}"."{table}"'

    delete_sql = f'DELETE FROM {fqtn} WHERE "_source_file" = %s;'
    # PostgresHook.run con return value requiere cursor; usamos get_first
    # para obtener el rowcount via RETURNING
    count_sql = f'SELECT COUNT(*) FROM {fqtn} WHERE "_source_file" = %s;'
    before = hook.get_first(count_sql, parameters=(source_file,))[0] or 0

    if before > 0:
        hook.run(delete_sql, parameters=(source_file,))
        logger.info(f"  Eliminadas {before:,} filas previas de {source_file}")

    return before


def load_dataframe_to_bronze(
    hook: PostgresHook,
    df: pd.DataFrame,
    perfil: Perfil,
    chunk_size: int = 10_000,
) -> int:
    """
    Inserta el DataFrame en la tabla Bronze correspondiente.

    Usa insert_rows() por chunks para manejar datasets grandes sin
    agotar memoria ni exceder límites de Postgres (65535 parámetros/query).

    Args:
        hook: PostgresHook al DWH.
        df: DataFrame con columnas canónicas + linaje.
        perfil: 'matriculados' o 'docentes'.
        chunk_size: filas por batch.

    Returns:
        Número total de filas insertadas.
    """
    table = BRONZE_TABLES[perfil]
    target = f"{BRONZE_SCHEMA}.{table}"

    # Convertir DataFrame a lista de tuplas con los mismos tipos que la tabla
    columns = list(df.columns)

    # Reemplazar NaN/None correctamente para Postgres
    df_clean = df.where(pd.notnull(df), None)
    rows = [tuple(r) for r in df_clean.itertuples(index=False, name=None)]

    hook.insert_rows(
        table=target,
        rows=rows,
        target_fields=columns,
        commit_every=chunk_size,
    )

    logger.info(f"✓ Insertadas {len(rows):,} filas en {target}")
    return len(rows)


# =============================================================================
# Logging de ingesta en bronze._ingestion_log
# =============================================================================

def log_ingestion_start(
    hook: PostgresHook,
    dag_run_id: str,
    perfil: str,
    anio: int,
    source_url: str,
    source_file: str,
) -> int:
    """
    Registra el inicio de una ingesta. Retorna el ID del log para actualizarlo después.
    """
    sql = """
        INSERT INTO bronze._ingestion_log
            (dag_run_id, perfil, anio, source_url, source_file, status, started_at)
        VALUES (%s, %s, %s, %s, %s, 'running', CURRENT_TIMESTAMP)
        RETURNING id;
    """
    result = hook.get_first(
        sql,
        parameters=(dag_run_id, perfil, anio, source_url, source_file),
    )
    return result[0]


def log_ingestion_success(
    hook: PostgresHook,
    log_id: int,
    rows_ingested: int,
) -> None:
    """Actualiza el log al finalizar exitosamente."""
    sql = """
        UPDATE bronze._ingestion_log
        SET status = 'success',
            rows_ingested = %s,
            finished_at = CURRENT_TIMESTAMP
        WHERE id = %s;
    """
    hook.run(sql, parameters=(rows_ingested, log_id))


def log_ingestion_failure(
    hook: PostgresHook,
    log_id: int,
    error_message: str,
) -> None:
    """Actualiza el log al fallar."""
    sql = """
        UPDATE bronze._ingestion_log
        SET status = 'failed',
            error_message = %s,
            finished_at = CURRENT_TIMESTAMP
        WHERE id = %s;
    """
    hook.run(sql, parameters=(error_message[:2000], log_id))  # trunc por seguridad


# =============================================================================
# API de alto nivel (lo que el DAG llama)
# =============================================================================

def load_to_bronze(
    df: pd.DataFrame,
    perfil: Perfil,
    anio: int,
    dag_run_id: str,
    postgres_conn_id: str = "snies_dwh",
) -> dict:
    """
    Pipeline completo de carga a Bronze.

    Pasos:
        1. Abre log de ingesta
        2. Asegura que la tabla Bronze exista (crea o altera)
        3. Borra datos previos del mismo source_file (idempotencia)
        4. Inserta el DataFrame
        5. Cierra el log con success/failure

    Args:
        df: DataFrame enriquecido con linaje.
        perfil: 'matriculados' o 'docentes'.
        anio: año que se está cargando.
        dag_run_id: ID de ejecución del DAG (ctx['dag_run'].run_id).
        postgres_conn_id: ID de la conexión Airflow al DWH.

    Returns:
        dict con estadísticas de la carga.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    source_url = df["_source_url"].iloc[0]
    source_file = df["_source_file"].iloc[0]

    log_id = log_ingestion_start(
        hook,
        dag_run_id=dag_run_id,
        perfil=perfil,
        anio=anio,
        source_url=source_url,
        source_file=source_file,
    )

    try:
        ensure_bronze_table_exists(hook, perfil=perfil, df=df)
        deleted = delete_existing_partition(hook, perfil=perfil, source_file=source_file)
        inserted = load_dataframe_to_bronze(hook, df=df, perfil=perfil)

        log_ingestion_success(hook, log_id=log_id, rows_ingested=inserted)

        return {
            "status": "success",
            "perfil": perfil,
            "anio": anio,
            "rows_deleted": deleted,
            "rows_inserted": inserted,
            "source_file": source_file,
        }

    except Exception as e:
        log_ingestion_failure(hook, log_id=log_id, error_message=str(e))
        logger.exception(f"❌ Falló la carga de {perfil} {anio}")
        raise
