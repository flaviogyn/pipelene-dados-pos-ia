/*
    fct_recordings
    Grão: uma linha por gravação. Vem direto da silver -- metadados
    e features acústicas já chegam juntos no mesmo arquivo parquet,
    então não há join a fazer aqui (diferente da versão anterior,
    que juntava bronze + silver).

    materialized/schema herdados de dbt_project.yml (marts: table, schema: gold)
*/

select
    recording_id,
    genus,
    species,
    subspecies,
    common_name,
    identification_status,
    country,
    location,
    latitude,
    longitude,
    altitude_m,
    quality_rating,
    xeno_canto_url,
    recording_date,
    recording_type,
    duration_seconds,

    (genus = '{{ var("target_genus") }}'
        and species = '{{ var("target_species") }}')   as is_target_species,

    energy_mean,
    rms_mean,
    rms_std,
    zcr_mean,
    zcr_std,
    spectral_centroid_mean,
    spectral_bandwidth_mean,
    mfcc_1_mean, mfcc_2_mean, mfcc_3_mean, mfcc_4_mean, mfcc_5_mean,
    mfcc_6_mean, mfcc_7_mean, mfcc_8_mean, mfcc_9_mean, mfcc_10_mean,
    mfcc_11_mean, mfcc_12_mean, mfcc_13_mean, mfcc_14_mean, mfcc_15_mean,
    mfcc_16_mean, mfcc_17_mean, mfcc_18_mean, mfcc_19_mean, mfcc_20_mean,
    mfcc_1_std, mfcc_2_std, mfcc_3_std, mfcc_4_std, mfcc_5_std,
    mfcc_6_std, mfcc_7_std, mfcc_8_std, mfcc_9_std, mfcc_10_std,
    mfcc_11_std, mfcc_12_std, mfcc_13_std, mfcc_14_std, mfcc_15_std,
    mfcc_16_std, mfcc_17_std, mfcc_18_std, mfcc_19_std, mfcc_20_std

from {{ ref('stg_recording_features') }}
