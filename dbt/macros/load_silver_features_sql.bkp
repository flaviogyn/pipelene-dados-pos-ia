{#
    Macro: load_silver_features
    Carrega todos os arquivos parquet (um por gravação) do stage
    S3_STAGE_SILVER para XENO_DB.RAW.SRC_RECORDING_FEATURES.

    Diferente da bronze (JSON -> VARIANT), o parquet já chega tipado,
    entao usamos MATCH_BY_COLUMN_NAME para mapear direto pelas
    colunas do arquivo -> colunas da tabela, sem precisar listar
    a projeção manualmente.

    Uso: dbt run-operation load_silver_features
#}

{% macro load_silver_features(force=false) %}

    {% set force_clause = 'FORCE = TRUE' if force else '' %}

    {% set copy_sql %}
        COPY INTO {{ source('xeno_raw', 'src_recording_features') }}
        FROM @{{ source('xeno_raw', 'src_recording_features').database }}.RAW.S3_STAGE_SILVER
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PATTERN = '.*\\.parquet'
        ON_ERROR = 'CONTINUE'
        {{ force_clause }}
        ;
    {% endset %}

    {% if execute %}
        {% set result = run_query(copy_sql) %}
        {{ log("COPY INTO (silver) finalizado. Linhas retornadas: " ~ result.rows, info=True) }}
    {% endif %}

{% endmacro %}
