-- =============================================================================
-- SNIES Data Warehouse - Inicialización
-- =============================================================================
-- Este script se ejecuta automáticamente la PRIMERA vez que arranca el
-- contenedor de Postgres (via docker-entrypoint-initdb.d).
-- Crea los tres schemas del patrón Medallion y los permisos necesarios.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Schemas del Medallion (Bronze / Silver / Gold)
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS
    'Capa de datos crudos. Preserva archivos SNIES tal como llegan, '
    'con columnas de linaje (_ingested_at, _source_file, _row_hash).';

COMMENT ON SCHEMA silver IS
    'Capa de datos limpios. Nulos tratados, nombres IES normalizados, '
    'filtro geográfico Bogotá aplicado, tipos de datos fuertes.';

COMMENT ON SCHEMA gold IS
    'Capa de consumo. Star schema con dimensiones (dim_ies, dim_tiempo) '
    'y hechos (fact_capacidad_academica). Optimizado para BI.';

-- -----------------------------------------------------------------------------
-- 2. Permisos: el usuario de la app (DWH_POSTGRES_USER) es dueño de todo
--    y tiene permisos completos sobre los tres schemas.
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON SCHEMA bronze TO CURRENT_USER;
GRANT ALL PRIVILEGES ON SCHEMA silver TO CURRENT_USER;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO CURRENT_USER;

-- Privilegios por defecto sobre futuras tablas creadas por dbt
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO CURRENT_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES TO CURRENT_USER;

-- -----------------------------------------------------------------------------
-- 3. Extensiones útiles
-- -----------------------------------------------------------------------------
-- pg_trgm: búsquedas fuzzy sobre nombres de IES (útil para normalización)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- -----------------------------------------------------------------------------
-- 4. Tabla de control de ingestas (útil para idempotencia y auditoría)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze._ingestion_log (
    id              SERIAL PRIMARY KEY,
    dag_run_id      TEXT NOT NULL,
    perfil          TEXT NOT NULL,          -- 'matriculados' | 'docentes'
    anio            INTEGER NOT NULL,
    source_url      TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    rows_ingested   INTEGER,
    status          TEXT NOT NULL,          -- 'success' | 'failed'
    error_message   TEXT,
    started_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_perfil_anio
    ON bronze._ingestion_log (perfil, anio);

COMMENT ON TABLE bronze._ingestion_log IS
    'Registro de cada ejecución de ingesta. Permite responder: '
    '¿cuándo se cargó X año? ¿cuántas filas? ¿alguna falló?';

-- -----------------------------------------------------------------------------
-- Fin del script. Las tablas de Bronze las crea el DAG de Airflow
-- (schema-on-read, se adaptan al archivo que llega).
-- Las tablas de Silver y Gold las crea dbt.
-- -----------------------------------------------------------------------------
