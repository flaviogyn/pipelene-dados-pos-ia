{#
    Macro: unload_dim_species
    Exporta o conteúdo atual de GOLD.DIM_SPECIES de volta pro S3,
    como um único arquivo parquet: s3://xeno-canto-s3/gold/dim_species.parquet

    Mesmo padrão de unload_fct_recordings: SINGLE = TRUE gera um
    arquivo com nome exato (sem sufixo de partição), OVERWRITE = TRUE
    substitui a versão anterior a cada execução.

    Uso: dbt run-operation unload_dim_species
#}

{% macro unload_dim_species() %}

    {% set unload_sql %}
        COPY INTO @XENO_DB.RAW.S3_STAGE_GOLD/dim_species.parquet
        FROM {{ ref('dim_species') }}
        FILE_FORMAT = (TYPE = PARQUET)
        OVERWRITE = TRUE
        SINGLE = TRUE
        MAX_FILE_SIZE = 5368709120
        ;
    {% endset %}

    {% if execute %}
        {% set result = run_query(unload_sql) %}
        {{ log("Unload de dim_species para S3 finalizado. " ~ result.rows, info=True) }}
    {% endif %}

{% endmacro %}
