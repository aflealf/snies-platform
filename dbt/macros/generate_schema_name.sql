{# =============================================================================
   Macro: generate_schema_name (OVERRIDE)
   =============================================================================
   Sobrescribe el comportamiento por defecto de dbt, que concatena el schema
   base del profile con el custom_schema del modelo: `<base>_<custom>`.

   Con esta macro:
       +schema: silver  →  crea el schema 'silver' (literal)
       +schema: gold    →  crea el schema 'gold'   (literal)
       (sin +schema)    →  usa el schema del profile (public)

   Docs oficiales:
   https://docs.getdbt.com/docs/build/custom-schemas#changing-the-way-dbt-generates-a-schema-name
   ============================================================================= #}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
