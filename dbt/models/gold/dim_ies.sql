{{
    config(
        materialized='table',
        schema='gold',
        indexes=[
            {'columns': ['codigo_snies'], 'unique': True},
            {'columns': ['es_sue']}
        ]
    )
}}

-- =============================================================================
-- dim_ies
-- =============================================================================
-- Dimensión Gold de las IES (universidades de Bogotá).
--
-- Construcción:
--     stg_ies      (todas las IES del país)
--       LEFT JOIN
--     ies_sue      (lookup de las 34 universidades estatales)
--       FILTER
--     Universidad + IU en Bogotá
--
-- Atributo clave: es_sue (BOOLEAN) — el "Atributo Plus" del reto.
-- =============================================================================

WITH ies_universidades_bogota AS (
    SELECT *
    FROM {{ ref('stg_ies') }}
    WHERE {{ es_bogota('departamento_ies') }}
      AND {{ es_universidad('caracter_ies') }}
),

enriched AS (
    SELECT
        ies.codigo_snies,
        ies.nombre_ies,
        ies.tipo_ies,
        ies.sector_ies,
        ies.caracter_ies,
        ies.departamento_ies,
        ies.municipio_ies,
        COALESCE(ies.ies_acreditada, 'No') AS ies_acreditada,

        -- Atributo SUE (Plus del reto)
        CASE
            WHEN sue.codigo_snies IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS es_sue,

        -- Metadata
        CURRENT_TIMESTAMP AS fecha_ultima_carga

    FROM ies_universidades_bogota ies
    LEFT JOIN {{ ref('ies_sue') }} sue
        ON ies.codigo_snies = sue.codigo_snies
)

SELECT * FROM enriched
