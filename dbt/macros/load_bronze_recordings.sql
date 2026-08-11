{#
    Macro: load_bronze_recordings
    Executa o COPY INTO que le todos os arquivos bird-sp-*.json
    presentes no stage S3 (pasta bronze/) e carrega na tabela
    XENO_DB.RAW.SRC_RECORDINGS.

    Uso (dentro do container dbt, disparado pelo Airflow via SSHOperator):
        dbt run-operation load_bronze_recordings

    Idempotencia: o COPY INTO do Snowflake ja controla, por padrao,
    quais arquivos foram carregados (nao recarrega o mesmo arquivo
    duas vezes). Para forcar recarga em teste, chame com force=true:
        dbt run-operation load_bronze_recordings --args '{force: true}'
#}

{% macro load_bronze_recordings(force=false) %}

    {% set force_clause = 'FORCE = TRUE' if force else '' %}

    {% set copy_sql %}
        COPY INTO {{ source('xeno_raw', 'src_recordings') }} (raw_data, file_name)
        FROM (
            SELECT
                $1                    AS raw_data,
                METADATA$FILENAME     AS file_name
            FROM @{{ source('xeno_raw', 'src_recordings').database }}.RAW.S3_STAGE
        )
        FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = TRUE)
        PATTERN = '.*birds-sp-.*\\.json'
        ON_ERROR = 'CONTINUE'
        {{ force_clause }}
        ;
    {% endset %}

    {% if execute %}
        {% set result = run_query(copy_sql) %}
        {{ log("COPY INTO finalizado. Linhas retornadas: " ~ result.rows, info=True) }}
    {% endif %}

{% endmacro %}