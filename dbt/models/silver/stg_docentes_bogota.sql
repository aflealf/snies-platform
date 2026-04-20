{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- =============================================================================
-- stg_docentes_bogota
-- =============================================================================
-- Docentes agregados por (IES × año), filtrados a Bogotá y universidades.
--
-- Misma lógica que stg_matriculados_bogota: SUM de semestres/dedicaciones/etc.
-- =============================================================================

WITH raw AS (
    SELECT
        {{ safe_cast_int('codigo_institucion') }}   AS codigo_snies,
        {{ safe_cast_int('anio') }}                 AS anio,
        {{ safe_cast_int('semestre') }}             AS semestre,
        {{ safe_cast_int('docentes') }}             AS docentes,
        departamento_domicilio_ies,
        caracter_ies
    FROM {{ source('bronze', 'docentes_raw') }}
    WHERE codigo_institucion IS NOT NULL
      AND anio IS NOT NULL
),

filtered AS (
    SELECT *
    FROM raw
    WHERE {{ es_bogota('departamento_domicilio_ies') }}
      AND {{ es_universidad('caracter_ies') }}
      AND codigo_snies IS NOT NULL
      AND anio IS NOT NULL
      AND docentes IS NOT NULL
      AND docentes >= 0
),

aggregated AS (
    SELECT
        codigo_snies,
        anio,
        SUM(docentes)                    AS total_docentes,
        COUNT(*)                          AS registros_fuente,
        COUNT(DISTINCT semestre)          AS semestres_presentes
    FROM filtered
    GROUP BY codigo_snies, anio
)

SELECT * FROM aggregated
