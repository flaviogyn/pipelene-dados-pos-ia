{{
    config(
        materialized='view'
    )
}}

/*
    stg_recording_features
    Camada staging da silver: renomeia a chave para recording_id
    (VARCHAR) e passa metadados + features acústicas adiante sem
    transformação -- o parquet já chega tipado e já traz tudo
    (metadados + features) no mesmo arquivo, então esta view alimenta
    a gold diretamente, sem join.

    Fonte: XENO_DB.RAW.SRC_RECORDING_FEATURES (carregada via macro
    load_silver_features)
*/

select
    id::varchar                     as recording_id,
    gen                             as genus,
    sp                              as species,
    nullif(ssp, '')                 as subspecies,
    en                              as common_name,
    status                          as identification_status,
    cnt                             as country,
    loc                             as location,
    q                                as quality_rating,
    url                              as xeno_canto_url,
    date                             as recording_date,
    type                             as recording_type,
    grp                             as taxonomic_group,
    lat                             as latitude,
    lon                             as longitude,
    alt                             as altitude_m,
    duration_seconds,

    -- energia / envelope
    energy_mean,
    rms_mean,
    rms_std,
    zcr_mean,
    zcr_std,

    -- espectro
    spectral_centroid_mean,
    spectral_bandwidth_mean,

    -- MFCCs (coeficientes 1 a 20, média e desvio padrão)
    mfcc_1_mean, mfcc_2_mean, mfcc_3_mean, mfcc_4_mean, mfcc_5_mean,
    mfcc_6_mean, mfcc_7_mean, mfcc_8_mean, mfcc_9_mean, mfcc_10_mean,
    mfcc_11_mean, mfcc_12_mean, mfcc_13_mean, mfcc_14_mean, mfcc_15_mean,
    mfcc_16_mean, mfcc_17_mean, mfcc_18_mean, mfcc_19_mean, mfcc_20_mean,
    mfcc_1_std, mfcc_2_std, mfcc_3_std, mfcc_4_std, mfcc_5_std,
    mfcc_6_std, mfcc_7_std, mfcc_8_std, mfcc_9_std, mfcc_10_std,
    mfcc_11_std, mfcc_12_std, mfcc_13_std, mfcc_14_std, mfcc_15_std,
    mfcc_16_std, mfcc_17_std, mfcc_18_std, mfcc_19_std, mfcc_20_std,

    source_file_name  as feature_source_file_name,
    loaded_at          as feature_loaded_at

from {{ source('xeno_raw', 'src_recording_features') }}
