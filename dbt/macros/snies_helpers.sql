{# =============================================================================
   Macros para la plataforma SNIES
   =============================================================================
   Utilidades reutilizables a lo largo de los modelos.
   ============================================================================= #}


{# -----------------------------------------------------------------------------
   Filtro de Bogotá: case-insensitive y tolerante a variaciones de formato.
   El SNIES usa 'Bogotá D.C.' (2022) y 'Bogotá, D.C.' (2023+), con y sin tildes.
   ----------------------------------------------------------------------------- #}
{% macro es_bogota(column_name='departamento_domicilio_ies') -%}
    UPPER({{ column_name }}) LIKE '{{ var("bogota_pattern") }}'
{%- endmacro %}


{# -----------------------------------------------------------------------------
   Filtro de universidades: UNIVERSIDAD + INSTITUCION UNIVERSITARIA.
   Decisión de negocio: dbt_project.yml:vars.caracter_universidad.
   ----------------------------------------------------------------------------- #}
{% macro es_universidad(column_name='caracter_ies') -%}
    {{ column_name }} IN (
        {% for c in var('caracter_universidad') -%}
            '{{ c }}'{% if not loop.last %}, {% endif %}
        {%- endfor %}
    )
{%- endmacro %}


{# -----------------------------------------------------------------------------
   Parseo seguro a entero. Los archivos SNIES a veces traen comas o espacios.
   Si el valor es nulo/vacío/basura, retorna NULL en vez de fallar.
   ----------------------------------------------------------------------------- #}
{% macro safe_cast_int(column_name) -%}
    NULLIF(
        REGEXP_REPLACE(COALESCE({{ column_name }}, ''), '[^0-9-]', '', 'g'),
        ''
    )::INTEGER
{%- endmacro %}
