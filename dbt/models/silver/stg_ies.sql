{{
    config(
        materialized='view',
        schema='silver'
    )
}}

-- =============================================================================
-- stg_ies
-- =============================================================================
-- Dimensión de IES deduplicada a partir de Bronze.
-- 
-- Estrategia:
--     Las tablas Bronze tienen la misma IES repetida miles de veces (una fila
--     por programa/nivel/sexo). Aquí extraemos la información ATRIBUTO de la
--     IES (sus características, no sus matriculados) y deduplicamos por código.
--
-- Fuentes: bronze.matriculados_raw UNION bronze.docentes_raw
--     (algunas IES aparecen solo en uno de los dos; hacemos UNION para cubrir todo)
--
-- Clave: codigo_institucion (código SNIES numérico, estable entre años)
-- =============================================================================

WITH ies_from_matriculados AS (
    SELECT DISTINCT
        codigo_institucion,
        institucion_educacion_superior_ies,
        tipo_ies,
        sector_ies,
        caracter_ies,
        departamento_domicilio_ies,
        municipio_domicilio_ies,
        ies_acreditada,
        anio
    FROM {{ source('bronze', 'matriculados_raw') }}
    WHERE codigo_institucion IS NOT NULL
      AND institucion_educacion_superior_ies IS NOT NULL
),

ies_from_docentes AS (
    SELECT DISTINCT
        codigo_institucion,
        institucion_educacion_superior_ies,
        tipo_ies,
        sector_ies,
        caracter_ies,
        departamento_domicilio_ies,
        municipio_domicilio_ies,
        ies_acreditada,
        anio
    FROM {{ source('bronze', 'docentes_raw') }}
    WHERE codigo_institucion IS NOT NULL
      AND institucion_educacion_superior_ies IS NOT NULL
),

combined AS (
    SELECT * FROM ies_from_matriculados
    UNION
    SELECT * FROM ies_from_docentes
),

-- Para cada codigo_institucion, tomamos la versión MÁS RECIENTE de sus atributos.
-- Esto maneja el caso en que una IES cambia de nombre o estado entre años:
-- la verdad canónica es el último registro disponible.
deduplicated AS (
    SELECT
        {{ safe_cast_int('codigo_institucion') }} AS codigo_snies,
        institucion_educacion_superior_ies AS nombre_ies,
        tipo_ies,
        sector_ies,
        caracter_ies,
        departamento_domicilio_ies AS departamento_ies,
        municipio_domicilio_ies AS municipio_ies,
        ies_acreditada,
        {{ safe_cast_int('anio') }} AS anio_ultimo_registro,
        ROW_NUMBER() OVER (
            PARTITION BY codigo_institucion
            ORDER BY {{ safe_cast_int('anio') }} DESC NULLS LAST,
                     institucion_educacion_superior_ies
        ) AS rn
    FROM combined
)

SELECT
    codigo_snies,
    nombre_ies,
    tipo_ies,

    -- NORMALIZACIÓN: el SNIES escribe el sector de forma inconsistente entre
    -- registros y departamentos. Lo canonicalizamos aquí en Silver.
    --   'Privado' / 'privado' / 'Privada'  →  'Privada'
    --   'Oficial' / 'oficial' / 'Público'  →  'Oficial'
    CASE
        WHEN UPPER(sector_ies) IN ('PRIVADO', 'PRIVADA') THEN 'Privada'
        WHEN UPPER(sector_ies) IN ('OFICIAL', 'PÚBLICO', 'PUBLICO') THEN 'Oficial'
        ELSE sector_ies   -- deja cualquier otro valor tal cual para detección
    END AS sector_ies,

    caracter_ies,
    departamento_ies,
    municipio_ies,
    ies_acreditada,
    anio_ultimo_registro
FROM deduplicated
WHERE rn = 1                        -- uno por código SNIES, el más reciente
  AND codigo_snies IS NOT NULL      -- descarta filas basura
