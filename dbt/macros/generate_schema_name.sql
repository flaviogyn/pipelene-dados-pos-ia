{#
    Override padrão do dbt: por default, quando um modelo define
    +schema: staging, o dbt cria a schema como '<target_schema>_staging'
    (ex: RAW_staging). Aqui forçamos o uso do nome exato definido em
    +schema, para termos schemas limpas: RAW, STAGING, MARTS, etc.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}