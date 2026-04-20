{{
    config(
        materialized='table',
        schema='gold',
        indexes=[
            {'columns': ['codigo_snies', 'anio'], 'unique': True},
            {'columns': ['anio']}
        ]
    )
}}

-- =============================================================================
-- fact_capacidad_academica
-- =============================================================================
-- ⭐ HECHO PRINCIPAL: Relación Estudiante por Docente de las universidades
-- de Bogotá por año (2022-2024). Responde la pregunta del reto.
--
-- Granularidad:  una fila por (codigo_snies × anio)
--
-- Métrica principal:
--     ratio_estudiante_docente = total_matriculados / total_docentes
--
-- Diseño:
--     INNER JOIN entre stg_matriculados_bogota y stg_docentes_bogota.
--     Si una IES tiene matriculados pero no docentes (o viceversa) para un
--     año, queda fuera del fact. Esto es correcto: no se puede calcular el
--     ratio sin ambos.
--
-- Qué queda documentado en los logs/tests:
--     - IES presentes solo en matriculados (sin docentes): se detectan
--       como "huérfanos" en los tests de dbt.
-- =============================================================================

WITH matriculados AS (
    SELECT
        codigo_snies,
        anio,
        total_matriculados
    FROM {{ ref('stg_matriculados_bogota') }}
),

docentes AS (
    SELECT
        codigo_snies,
        anio,
        total_docentes
    FROM {{ ref('stg_docentes_bogota') }}
),

joined AS (
    SELECT
        m.codigo_snies,
        m.anio,
        m.total_matriculados,
        d.total_docentes
    FROM matriculados m
    INNER JOIN docentes d
        ON m.codigo_snies = d.codigo_snies
        AND m.anio = d.anio
),

-- Filtramos también a IES que están en dim_ies (Universidad + IU de Bogotá).
-- Esto asegura integridad referencial: todo row del fact tiene su IES en dim.
with_ies_filter AS (
    SELECT j.*
    FROM joined j
    INNER JOIN {{ ref('dim_ies') }} d USING (codigo_snies)
)

SELECT
    codigo_snies,
    anio,
    total_matriculados,
    total_docentes,

    -- ⭐ Métrica principal del reto
    CASE
        WHEN total_docentes > 0
            THEN ROUND(total_matriculados::NUMERIC / total_docentes, 2)
        ELSE NULL
    END AS ratio_estudiante_docente,

    -- Flags de calidad (útiles en BI para resaltar outliers)
    CASE
        WHEN total_docentes = 0           THEN 'sin_docentes'
        WHEN total_matriculados = 0       THEN 'sin_matriculados'
        WHEN total_docentes < 10          THEN 'docentes_bajos'
        WHEN total_matriculados < 100     THEN 'matriculados_bajos'
        ELSE 'ok'
    END AS flag_calidad,

    CURRENT_TIMESTAMP AS fecha_calculo

FROM with_ies_filter
