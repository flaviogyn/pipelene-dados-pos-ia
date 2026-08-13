{#
    Macro: unload_fct_recordings
    Exporta o conteúdo atual de GOLD.FCT_RECORDINGS de volta pro S3,
    como um único arquivo parquet: s3://xeno-canto-s3/gold/fct_recordings.parquet

    SINGLE = TRUE força um arquivo único com esse nome exato (sem isso,
    o Snowflake particiona a saída em vários arquivos com sufixo
    _0_0_0.snappy.parquet, um por thread do warehouse).
    OVERWRITE = TRUE garante que cada execução substitui o arquivo
    anterior, em vez de acumular versões.

    Uso: dbt run-operation unload_fct_recordings
#}

{% macro unload_fct_recordings() %}

    {% set unload_sql %}
        COPY INTO @XENO_DB.RAW.S3_STAGE_GOLD/fct_recordings.parquet
        FROM {{ ref('fct_recordings') }}
        FILE_FORMAT = (TYPE = PARQUET)
        OVERWRITE = TRUE
        SINGLE = TRUE
        MAX_FILE_SIZE = 5368709120
        ;
    {% endset %}

    {% if execute %}
        {% set result = run_query(unload_sql) %}
        {{ log("Unload de fct_recordings para S3 finalizado. " ~ result.rows, info=True) }}
    {% endif %}

{% endmacro %}
