{#
    Macro: load_silver_features
    Faz UPSERT (MERGE) dos parquets da silver direto do stage pra
    XENO_DB.RAW.SRC_RECORDING_FEATURES.

    Por que MERGE e não COPY INTO:
    Se um arquivo <id>.parquet for regravado no S3 com valores
    atualizados (features recalculadas, correção de metadados etc.),
    um COPY INTO simples duplicaria a linha -- o Snowflake percebe
    que o checksum do arquivo mudou e recarrega, mas COPY INTO só
    sabe inserir, não sabe que aquele "id" já existe.

    O MERGE resolve isso: registros com "id" já existente são
    atualizados (UPDATE); registros novos são inseridos (INSERT).
    Roda com segurança quantas vezes for chamado (idempotente).

    Uso: dbt run-operation load_silver_features
#}

{% macro load_silver_features() %}

    {% set merge_sql %}
        MERGE INTO {{ source('xeno_raw', 'src_recording_features') }} AS target
        USING (
            SELECT
                $1:id::NUMBER                          AS id,
                $1:gen::STRING                         AS gen,
                $1:sp::STRING                          AS sp,
                $1:ssp::STRING                         AS ssp,
                $1:en::STRING                          AS en,
                $1:status::STRING                      AS status,
                $1:cnt::STRING                         AS cnt,
                $1:loc::STRING                         AS loc,
                $1:q::STRING                           AS q,
                $1:url::STRING                         AS url,
                COALESCE(
                    TRY_TO_DATE($1:date::STRING, 'YYYY-MM-DD'),
                    CURRENT_DATE()
                )                                     AS date,
                $1:type::STRING                        AS type,
                $1:lat::FLOAT                          AS lat,
                $1:lon::FLOAT                          AS lon,
                $1:alt::NUMBER                         AS alt,
                $1:grp::STRING                         AS grp,
                $1:duration_seconds::FLOAT             AS duration_seconds,
                $1:energy_mean::FLOAT                  AS energy_mean,
                $1:rms_mean::FLOAT                     AS rms_mean,
                $1:rms_std::FLOAT                      AS rms_std,
                $1:zcr_mean::FLOAT                     AS zcr_mean,
                $1:zcr_std::FLOAT                      AS zcr_std,
                $1:spectral_centroid_mean::FLOAT       AS spectral_centroid_mean,
                $1:spectral_bandwidth_mean::FLOAT      AS spectral_bandwidth_mean,
                $1:mfcc_1_mean::FLOAT  AS mfcc_1_mean,  $1:mfcc_2_mean::FLOAT  AS mfcc_2_mean,
                $1:mfcc_3_mean::FLOAT  AS mfcc_3_mean,  $1:mfcc_4_mean::FLOAT  AS mfcc_4_mean,
                $1:mfcc_5_mean::FLOAT  AS mfcc_5_mean,  $1:mfcc_6_mean::FLOAT  AS mfcc_6_mean,
                $1:mfcc_7_mean::FLOAT  AS mfcc_7_mean,  $1:mfcc_8_mean::FLOAT  AS mfcc_8_mean,
                $1:mfcc_9_mean::FLOAT  AS mfcc_9_mean,  $1:mfcc_10_mean::FLOAT AS mfcc_10_mean,
                $1:mfcc_11_mean::FLOAT AS mfcc_11_mean, $1:mfcc_12_mean::FLOAT AS mfcc_12_mean,
                $1:mfcc_13_mean::FLOAT AS mfcc_13_mean, $1:mfcc_14_mean::FLOAT AS mfcc_14_mean,
                $1:mfcc_15_mean::FLOAT AS mfcc_15_mean, $1:mfcc_16_mean::FLOAT AS mfcc_16_mean,
                $1:mfcc_17_mean::FLOAT AS mfcc_17_mean, $1:mfcc_18_mean::FLOAT AS mfcc_18_mean,
                $1:mfcc_19_mean::FLOAT AS mfcc_19_mean, $1:mfcc_20_mean::FLOAT AS mfcc_20_mean,
                $1:mfcc_1_std::FLOAT  AS mfcc_1_std,   $1:mfcc_2_std::FLOAT  AS mfcc_2_std,
                $1:mfcc_3_std::FLOAT  AS mfcc_3_std,   $1:mfcc_4_std::FLOAT  AS mfcc_4_std,
                $1:mfcc_5_std::FLOAT  AS mfcc_5_std,   $1:mfcc_6_std::FLOAT  AS mfcc_6_std,
                $1:mfcc_7_std::FLOAT  AS mfcc_7_std,   $1:mfcc_8_std::FLOAT  AS mfcc_8_std,
                $1:mfcc_9_std::FLOAT  AS mfcc_9_std,   $1:mfcc_10_std::FLOAT AS mfcc_10_std,
                $1:mfcc_11_std::FLOAT AS mfcc_11_std,  $1:mfcc_12_std::FLOAT AS mfcc_12_std,
                $1:mfcc_13_std::FLOAT AS mfcc_13_std,  $1:mfcc_14_std::FLOAT AS mfcc_14_std,
                $1:mfcc_15_std::FLOAT AS mfcc_15_std,  $1:mfcc_16_std::FLOAT AS mfcc_16_std,
                $1:mfcc_17_std::FLOAT AS mfcc_17_std,  $1:mfcc_18_std::FLOAT AS mfcc_18_std,
                $1:mfcc_19_std::FLOAT AS mfcc_19_std,  $1:mfcc_20_std::FLOAT AS mfcc_20_std,
                METADATA$FILENAME                      AS source_file_name
            FROM @{{ source('xeno_raw', 'src_recording_features').database }}.RAW.S3_STAGE_SILVER
                (PATTERN => '.*\\.parquet')
        ) AS source
        ON target.id = source.id

        WHEN MATCHED THEN UPDATE SET
            target.gen = source.gen,
            target.sp = source.sp,
            target.ssp = source.ssp,
            target.en = source.en,
            target.status = source.status,
            target.cnt = source.cnt,
            target.loc = source.loc,
            target.q = source.q,
            target.url = source.url,
            target.date = source.date,
            target.type = source.type,
            target.lat = source.lat,
            target.lon = source.lon,
            target.alt = source.alt,
            target.grp = source.grp,
            target.duration_seconds = source.duration_seconds,
            target.energy_mean = source.energy_mean,
            target.rms_mean = source.rms_mean,
            target.rms_std = source.rms_std,
            target.zcr_mean = source.zcr_mean,
            target.zcr_std = source.zcr_std,
            target.spectral_centroid_mean = source.spectral_centroid_mean,
            target.spectral_bandwidth_mean = source.spectral_bandwidth_mean,
            target.mfcc_1_mean = source.mfcc_1_mean,   target.mfcc_2_mean = source.mfcc_2_mean,
            target.mfcc_3_mean = source.mfcc_3_mean,   target.mfcc_4_mean = source.mfcc_4_mean,
            target.mfcc_5_mean = source.mfcc_5_mean,   target.mfcc_6_mean = source.mfcc_6_mean,
            target.mfcc_7_mean = source.mfcc_7_mean,   target.mfcc_8_mean = source.mfcc_8_mean,
            target.mfcc_9_mean = source.mfcc_9_mean,   target.mfcc_10_mean = source.mfcc_10_mean,
            target.mfcc_11_mean = source.mfcc_11_mean, target.mfcc_12_mean = source.mfcc_12_mean,
            target.mfcc_13_mean = source.mfcc_13_mean, target.mfcc_14_mean = source.mfcc_14_mean,
            target.mfcc_15_mean = source.mfcc_15_mean, target.mfcc_16_mean = source.mfcc_16_mean,
            target.mfcc_17_mean = source.mfcc_17_mean, target.mfcc_18_mean = source.mfcc_18_mean,
            target.mfcc_19_mean = source.mfcc_19_mean, target.mfcc_20_mean = source.mfcc_20_mean,
            target.mfcc_1_std = source.mfcc_1_std,   target.mfcc_2_std = source.mfcc_2_std,
            target.mfcc_3_std = source.mfcc_3_std,   target.mfcc_4_std = source.mfcc_4_std,
            target.mfcc_5_std = source.mfcc_5_std,   target.mfcc_6_std = source.mfcc_6_std,
            target.mfcc_7_std = source.mfcc_7_std,   target.mfcc_8_std = source.mfcc_8_std,
            target.mfcc_9_std = source.mfcc_9_std,   target.mfcc_10_std = source.mfcc_10_std,
            target.mfcc_11_std = source.mfcc_11_std, target.mfcc_12_std = source.mfcc_12_std,
            target.mfcc_13_std = source.mfcc_13_std, target.mfcc_14_std = source.mfcc_14_std,
            target.mfcc_15_std = source.mfcc_15_std, target.mfcc_16_std = source.mfcc_16_std,
            target.mfcc_17_std = source.mfcc_17_std, target.mfcc_18_std = source.mfcc_18_std,
            target.mfcc_19_std = source.mfcc_19_std, target.mfcc_20_std = source.mfcc_20_std,
            target.source_file_name = source.source_file_name,
            target.loaded_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT (
            id, gen, sp, ssp, en, status, cnt, loc, q, url, date, type,
            lat, lon, alt, grp, duration_seconds,
            energy_mean, rms_mean, rms_std, zcr_mean, zcr_std,
            spectral_centroid_mean, spectral_bandwidth_mean,
            mfcc_1_mean, mfcc_2_mean, mfcc_3_mean, mfcc_4_mean, mfcc_5_mean,
            mfcc_6_mean, mfcc_7_mean, mfcc_8_mean, mfcc_9_mean, mfcc_10_mean,
            mfcc_11_mean, mfcc_12_mean, mfcc_13_mean, mfcc_14_mean, mfcc_15_mean,
            mfcc_16_mean, mfcc_17_mean, mfcc_18_mean, mfcc_19_mean, mfcc_20_mean,
            mfcc_1_std, mfcc_2_std, mfcc_3_std, mfcc_4_std, mfcc_5_std,
            mfcc_6_std, mfcc_7_std, mfcc_8_std, mfcc_9_std, mfcc_10_std,
            mfcc_11_std, mfcc_12_std, mfcc_13_std, mfcc_14_std, mfcc_15_std,
            mfcc_16_std, mfcc_17_std, mfcc_18_std, mfcc_19_std, mfcc_20_std,
            source_file_name, loaded_at
        ) VALUES (
            source.id, source.gen, source.sp, source.ssp, source.en, source.status,
            source.cnt, source.loc, source.q, source.url, source.date, source.type,
            source.lat, source.lon, source.alt, source.grp, source.duration_seconds,
            source.energy_mean, source.rms_mean, source.rms_std, source.zcr_mean, source.zcr_std,
            source.spectral_centroid_mean, source.spectral_bandwidth_mean,
            source.mfcc_1_mean, source.mfcc_2_mean, source.mfcc_3_mean, source.mfcc_4_mean, source.mfcc_5_mean,
            source.mfcc_6_mean, source.mfcc_7_mean, source.mfcc_8_mean, source.mfcc_9_mean, source.mfcc_10_mean,
            source.mfcc_11_mean, source.mfcc_12_mean, source.mfcc_13_mean, source.mfcc_14_mean, source.mfcc_15_mean,
            source.mfcc_16_mean, source.mfcc_17_mean, source.mfcc_18_mean, source.mfcc_19_mean, source.mfcc_20_mean,
            source.mfcc_1_std, source.mfcc_2_std, source.mfcc_3_std, source.mfcc_4_std, source.mfcc_5_std,
            source.mfcc_6_std, source.mfcc_7_std, source.mfcc_8_std, source.mfcc_9_std, source.mfcc_10_std,
            source.mfcc_11_std, source.mfcc_12_std, source.mfcc_13_std, source.mfcc_14_std, source.mfcc_15_std,
            source.mfcc_16_std, source.mfcc_17_std, source.mfcc_18_std, source.mfcc_19_std, source.mfcc_20_std,
            source.source_file_name, current_timestamp()
        )
        ;
    {% endset %}

    {% if execute %}
        {% set result = run_query(merge_sql) %}
        {{ log("MERGE (silver) finalizado. Linhas afetadas: " ~ result.rows, info=True) }}
    {% endif %}

{% endmacro %}
