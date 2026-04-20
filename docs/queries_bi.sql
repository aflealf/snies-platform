-- =============================================================================
-- QUERIES BI PARA TABLEAU / METABASE
-- =============================================================================
-- Colección de queries listas para copiar-pegar en una herramienta BI.
-- Todas apuntan al schema `gold.*` del DWH.
--
-- Uso:
--   1. Conéctate a Postgres en localhost:5432 / snies_dwh
--   2. Selecciona el schema `gold`
--   3. Copia y pega cualquiera de estas queries
-- =============================================================================


-- =============================================================================
-- ⭐ QUERY 1 — La pregunta del reto (respuesta directa)
-- =============================================================================
-- Muestra todas las universidades de Bogotá con su ratio estudiante/docente
-- en los 3 años del análisis.

SELECT
    d.nombre_ies,
    d.sector_ies,
    d.es_sue,
    f.anio,
    f.total_matriculados,
    f.total_docentes,
    f.ratio_estudiante_docente
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE f.flag_calidad = 'ok'
ORDER BY f.anio DESC, f.ratio_estudiante_docente DESC;


-- =============================================================================
-- ⭐ QUERY 2 — Top 10 mejores ratios en 2024 (menor = más docentes por estudiante)
-- =============================================================================
-- Filtrando IES con al menos 50 docentes para evitar outliers de instituciones
-- muy pequeñas (posgrados, escuelas especializadas).

SELECT
    d.nombre_ies,
    d.sector_ies,
    CASE WHEN d.es_sue THEN '⭐ SUE' ELSE '' END AS tipo_sue,
    f.total_matriculados,
    f.total_docentes,
    f.ratio_estudiante_docente
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE f.anio = 2024
  AND f.flag_calidad = 'ok'
  AND f.total_docentes >= 50
ORDER BY f.ratio_estudiante_docente ASC
LIMIT 10;


-- =============================================================================
-- ⭐ QUERY 3 — Top 10 peores ratios en 2024 (mayor = sobresaturación)
-- =============================================================================
-- Estas IES tienen alta carga docente. Muchas corresponden a educación virtual.

SELECT
    d.nombre_ies,
    d.sector_ies,
    CASE WHEN d.es_sue THEN '⭐ SUE' ELSE '' END AS tipo_sue,
    f.total_matriculados,
    f.total_docentes,
    f.ratio_estudiante_docente
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE f.anio = 2024
  AND f.flag_calidad = 'ok'
ORDER BY f.ratio_estudiante_docente DESC
LIMIT 10;


-- =============================================================================
-- ⭐ QUERY 4 — Comparación SUE vs No-SUE a lo largo del tiempo
-- =============================================================================
-- Agrega todos los matriculados y docentes por tipo (SUE vs Privadas) y año.
-- Útil para ver la evolución del sistema público vs privado.

SELECT
    f.anio,
    CASE WHEN d.es_sue THEN 'SUE (públicas)' ELSE 'No SUE (privadas)' END AS tipo,
    COUNT(*) AS num_sedes,
    SUM(f.total_matriculados) AS matriculados_total,
    SUM(f.total_docentes) AS docentes_total,
    ROUND(
        SUM(f.total_matriculados)::NUMERIC / NULLIF(SUM(f.total_docentes), 0),
        2
    ) AS ratio_agregado
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
GROUP BY f.anio, d.es_sue
ORDER BY f.anio, tipo;


-- =============================================================================
-- ⭐ QUERY 5 — Las 6 sedes SUE de Bogotá: evolución temporal
-- =============================================================================
-- Enfoque en las universidades del Sistema Universitario Estatal en Bogotá.

SELECT
    d.nombre_ies,
    f.anio,
    f.total_matriculados,
    f.total_docentes,
    f.ratio_estudiante_docente,
    -- Variación porcentual vs año anterior
    ROUND(
        100.0 * (
            f.ratio_estudiante_docente - LAG(f.ratio_estudiante_docente) OVER (
                PARTITION BY d.codigo_snies ORDER BY f.anio
            )
        ) / NULLIF(LAG(f.ratio_estudiante_docente) OVER (
            PARTITION BY d.codigo_snies ORDER BY f.anio
        ), 0),
        2
    ) AS variacion_pct_vs_anio_anterior
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE d.es_sue = TRUE
ORDER BY d.nombre_ies, f.anio;


-- =============================================================================
-- ⭐ QUERY 6 — IES que mejoraron/empeoraron más su ratio (2022 → 2024)
-- =============================================================================
-- Comparación directa primero vs último año.
-- Una mejora = ratio bajó (más docentes por estudiante).

WITH ratios_anuales AS (
    SELECT
        d.nombre_ies,
        d.sector_ies,
        d.es_sue,
        MAX(CASE WHEN f.anio = 2022 THEN f.ratio_estudiante_docente END) AS ratio_2022,
        MAX(CASE WHEN f.anio = 2024 THEN f.ratio_estudiante_docente END) AS ratio_2024
    FROM gold.fact_capacidad_academica f
    JOIN gold.dim_ies d USING (codigo_snies)
    WHERE f.flag_calidad = 'ok'
    GROUP BY d.codigo_snies, d.nombre_ies, d.sector_ies, d.es_sue
)
SELECT
    nombre_ies,
    sector_ies,
    ratio_2022,
    ratio_2024,
    ROUND(ratio_2024 - ratio_2022, 2) AS delta,
    CASE
        WHEN ratio_2024 < ratio_2022 THEN '✓ Mejoró'
        WHEN ratio_2024 > ratio_2022 THEN '✗ Empeoró'
        ELSE '= Estable'
    END AS tendencia
FROM ratios_anuales
WHERE ratio_2022 IS NOT NULL AND ratio_2024 IS NOT NULL
ORDER BY delta ASC;


-- =============================================================================
-- ⭐ QUERY 7 — Distribución de ratios por sector
-- =============================================================================
-- Útil para histogramas en Tableau. Cada fila es una IES en 2024.

SELECT
    d.sector_ies,
    d.caracter_ies,
    d.nombre_ies,
    f.ratio_estudiante_docente,
    f.total_matriculados,
    f.total_docentes
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE f.anio = 2024
  AND f.flag_calidad = 'ok'
ORDER BY d.sector_ies, f.ratio_estudiante_docente;


-- =============================================================================
-- ⭐ QUERY 8 — Universidades más grandes de Bogotá (volumen)
-- =============================================================================
-- Top 20 por matriculados en 2024. No tiene que ver con el ratio, sino con
-- el tamaño absoluto. Contexto para interpretar los ratios.

SELECT
    d.nombre_ies,
    d.sector_ies,
    CASE WHEN d.es_sue THEN '⭐' ELSE '' END AS sue,
    f.total_matriculados,
    f.total_docentes,
    f.ratio_estudiante_docente,
    -- Porcentaje del total de matriculados en Bogotá
    ROUND(
        100.0 * f.total_matriculados / SUM(f.total_matriculados) OVER (),
        2
    ) AS porcentaje_matriculados
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE f.anio = 2024
ORDER BY f.total_matriculados DESC
LIMIT 20;


-- =============================================================================
-- ⭐ QUERY 9 — KPIs ejecutivos de Bogotá
-- =============================================================================
-- Una fila por año con los indicadores agregados.
-- Ideal para un dashboard de "vista gerencial".

SELECT
    f.anio,
    COUNT(DISTINCT f.codigo_snies) AS num_universidades,
    COUNT(DISTINCT f.codigo_snies) FILTER (WHERE d.es_sue) AS num_sue,
    SUM(f.total_matriculados) AS total_matriculados,
    SUM(f.total_docentes) AS total_docentes,
    ROUND(
        SUM(f.total_matriculados)::NUMERIC / NULLIF(SUM(f.total_docentes), 0),
        2
    ) AS ratio_promedio_ponderado,
    ROUND(AVG(f.ratio_estudiante_docente)::NUMERIC, 2) AS ratio_promedio_simple,
    ROUND(MIN(f.ratio_estudiante_docente), 2) AS ratio_minimo,
    ROUND(MAX(f.ratio_estudiante_docente), 2) AS ratio_maximo
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE f.flag_calidad = 'ok'
GROUP BY f.anio
ORDER BY f.anio;


-- =============================================================================
-- ⭐ QUERY 10 — Detección de outliers y casos especiales
-- =============================================================================
-- IES con ratios extremos o flags de calidad. Útil para auditoría.

SELECT
    d.nombre_ies,
    d.sector_ies,
    f.anio,
    f.total_matriculados,
    f.total_docentes,
    f.ratio_estudiante_docente,
    f.flag_calidad,
    CASE
        WHEN f.ratio_estudiante_docente > 50 THEN 'Muy alto (posible virtual)'
        WHEN f.ratio_estudiante_docente < 3  THEN 'Muy bajo (posgrado/especializada)'
        WHEN f.flag_calidad != 'ok'          THEN 'Requiere revisión'
        ELSE 'Normal'
    END AS categoria
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE f.ratio_estudiante_docente > 50
   OR f.ratio_estudiante_docente < 3
   OR f.flag_calidad != 'ok'
ORDER BY f.anio DESC, f.ratio_estudiante_docente DESC;


-- =============================================================================
-- 🔧 QUERIES DE VALIDACIÓN (para auditoría del DWH)
-- =============================================================================

-- Verificación del atributo SUE: debe haber 6 sedes SUE en Bogotá
SELECT codigo_snies, nombre_ies, sector_ies, es_sue
FROM gold.dim_ies
WHERE es_sue = TRUE
ORDER BY codigo_snies;
-- Esperado: 1101 UNAL, 1105 Pedagógica, 1117 Militar, 1121 Colegio Mayor,
--           1301 Distrital, 2102 UNAD


-- Última vez que se refrescó Gold
SELECT
    MAX(fecha_calculo) AS ultima_calculo_fact,
    (SELECT MAX(fecha_ultima_carga) FROM gold.dim_ies) AS ultima_carga_dim
FROM gold.fact_capacidad_academica;


-- Cobertura de datos: ¿hay años completos?
SELECT
    anio,
    COUNT(DISTINCT codigo_snies) AS num_ies
FROM gold.fact_capacidad_academica
GROUP BY anio
ORDER BY anio;


-- Estadísticas resumen por año
SELECT
    anio,
    COUNT(*) AS num_ies,
    ROUND(AVG(ratio_estudiante_docente)::NUMERIC, 2) AS promedio,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ratio_estudiante_docente)::NUMERIC, 2) AS mediana,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ratio_estudiante_docente)::NUMERIC, 2) AS p25,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ratio_estudiante_docente)::NUMERIC, 2) AS p75
FROM gold.fact_capacidad_academica
WHERE flag_calidad = 'ok'
GROUP BY anio
ORDER BY anio;
