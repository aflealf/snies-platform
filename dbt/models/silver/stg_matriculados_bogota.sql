{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- =============================================================================
-- stg_matriculados_bogota
-- =============================================================================
-- Matriculados agregados por (IES × año), filtrados a Bogotá y universidades.
--
-- Agregación: SUMA de todos los semestres, programas, niveles y sexos.
--     Esta es la convención del MEN: un estudiante matriculado en 2024-1 y
--     2024-2 cuenta como 2. Para el ratio estudiante/docente, la doble
--     contabilización se cancela porque docentes usa la misma suma.
--
-- Filtros aplicados:
--     1. Bogotá (cubre 'Bogotá D.C.' y 'Bogotá, D.C.')
--     2. Carácter: Universidad + Institución Universitaria
--     3. Código SNIES válido (numérico)
-- =============================================================================

WITH raw AS (
    SELECT
        {{ safe_cast_int('codigo_institucion') }}   AS codigo_snies,
        {{ safe_cast_int('anio') }}                 AS anio,
        {{ safe_cast_int('semestre') }}             AS semestre,
        {{ safe_cast_int('matriculados') }}         AS matriculados,
        departamento_domicilio_ies,
        caracter_ies
    FROM {{ source('bronze', 'matriculados_raw') }}
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
      AND matriculados IS NOT NULL
      AND matriculados >= 0
),

aggregated AS (
    SELECT
        codigo_snies,
        anio,
        SUM(matriculados)               AS total_matriculados,
        COUNT(*)                         AS registros_fuente,
        COUNT(DISTINCT semestre)         AS semestres_presentes
    FROM filtered
    GROUP BY codigo_snies, anio
)

SELECT * FROM aggregated
