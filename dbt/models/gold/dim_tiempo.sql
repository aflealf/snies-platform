{{
    config(
        materialized='table',
        schema='gold',
        indexes=[{'columns': ['anio'], 'unique': True}]
    )
}}

-- =============================================================================
-- dim_tiempo
-- =============================================================================
-- Dimensión temporal. Por ahora granularidad ANUAL (lo que pide el reto).
--
-- Se construye dinámicamente a partir de los años que existen en los hechos.
-- Así, cuando Airflow cargue 2025 al catálogo, esta dimensión se extiende
-- automáticamente sin cambios en dbt.
-- =============================================================================

WITH anios_disponibles AS (
    SELECT DISTINCT anio FROM {{ ref('stg_matriculados_bogota') }}
    UNION
    SELECT DISTINCT anio FROM {{ ref('stg_docentes_bogota') }}
)

SELECT
    anio,
    anio::TEXT                              AS anio_texto,
    anio - (anio % 10)                      AS decada,
    CASE
        WHEN anio % 4 = 0
         AND (anio % 100 != 0 OR anio % 400 = 0) THEN TRUE
        ELSE FALSE
    END                                     AS es_bisiesto,
    CURRENT_TIMESTAMP                       AS fecha_ultima_carga
FROM anios_disponibles
WHERE anio IS NOT NULL
ORDER BY anio
