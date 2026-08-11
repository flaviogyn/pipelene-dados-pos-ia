{{
    config(
        materialized='view'
    )
}}
-- materialized e schema já vêm herdados de dbt_project.yml
-- (models.pipelene_dbt.staging: +materialized: view, +schema: staging)
-- então não precisa repetir aqui; deixei só o materialized por clareza.

/*
    stg_recordings
    Camada staging: parsing tipado do VARIANT raw_data (bronze) para
    colunas com tipos corretos. Um registro por gravação do Xeno-canto.

    Fonte: XENO_DB.RAW.SRC_RECORDINGS (carregada via macro load_bronze_recordings)
    Destino: view XENO_DB.STAGING.STG_RECORDINGS (schema 'staging', ver
    a macro generate_schema_name em macros/generate_schema_name.sql)
*/

with source as (

    select *
    from {{ source('xeno_raw', 'src_recordings') }}

),

parsed as (

    select
        -- identificação
        raw_data:id::varchar                       as recording_id,
        raw_data:gen::varchar                       as genus,
        raw_data:sp::varchar                        as species,
        nullif(raw_data:ssp::varchar, '')            as subspecies,
        raw_data:grp::varchar                        as taxonomic_group,
        raw_data:status::varchar                     as identification_status,
        raw_data:en::varchar                         as common_name,

        -- quem gravou / onde
        raw_data:rec::varchar                        as recordist,
        raw_data:cnt::varchar                        as country,
        raw_data:loc::varchar                        as location,
        try_cast(raw_data:lat::varchar as float)      as latitude,
        try_cast(raw_data:lon::varchar as float)      as longitude,
        try_cast(raw_data:alt::varchar as float)      as altitude_m,

        -- características da gravação
        raw_data:type::varchar                        as recording_type,
        nullif(raw_data:sex::varchar, '')              as sex,
        nullif(raw_data:stage::varchar, '')            as life_stage,
        raw_data:method::varchar                       as recording_method,
        raw_data:q::varchar                            as quality_rating,

        -- duração (formato original "m:ss") e versão em segundos
        raw_data:length::varchar                       as duration_raw,
        try_cast(split_part(raw_data:length::varchar, ':', 1) as int) * 60
            + try_cast(split_part(raw_data:length::varchar, ':', 2) as int)
                                                         as duration_seconds,

        raw_data:time::varchar                          as recording_time,
        try_cast(raw_data:date::varchar as date)         as recording_date,
        try_cast(raw_data:uploaded::varchar as date)     as uploaded_date,

        -- links e mídia
        raw_data:url::varchar                            as xeno_canto_url,
        raw_data:file::varchar                            as audio_download_url,
        raw_data:"file-name"::varchar                     as audio_file_name,
        raw_data:sono:large::varchar                       as spectrogram_url,
        raw_data:lic::varchar                              as license_url,

        -- contexto adicional
        raw_data:also::array                                as also_species,
        nullif(raw_data:rmk::varchar, '')                    as remarks,
        raw_data:"animal-seen"::varchar                      as animal_seen,
        raw_data:"playback-used"::varchar                    as playback_used,
        try_cast(raw_data:temp::varchar as float)             as temperature_c,

        -- equipamento
        nullif(raw_data:dvc::varchar, '')                      as device,
        nullif(raw_data:mic::varchar, '')                      as microphone,
        try_cast(raw_data:smp::varchar as integer)             as sample_rate_hz,

        -- metadados de carga (herdados da bronze)
        file_name  as source_file_name,
        loaded_at  as bronze_loaded_at

    from source

)

select * from parsed
