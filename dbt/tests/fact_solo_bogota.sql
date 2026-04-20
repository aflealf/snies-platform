-- =============================================================================
-- Test: todas las IES en fact_capacidad_academica deben estar en Bogotá.
-- =============================================================================
-- Este test falla si por algún motivo una IES fuera de Bogotá se coló.
-- Es un test de invariante del negocio: el reto especifica Bogotá.
-- =============================================================================

SELECT f.codigo_snies, f.anio, d.departamento_ies
FROM {{ ref('fact_capacidad_academica') }} f
LEFT JOIN {{ ref('dim_ies') }} d USING (codigo_snies)
WHERE d.departamento_ies IS NULL
   OR UPPER(d.departamento_ies) NOT LIKE 'BOGOT%'
