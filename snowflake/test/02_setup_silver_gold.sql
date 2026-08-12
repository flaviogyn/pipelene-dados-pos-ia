-- ============================================================
-- Setup da Silver (parquet) + Gold (marts) + acesso do Metabase
-- Executar uma vez (ou quando credenciais/paths mudarem)
-- ============================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE XENO_WH;
USE DATABASE XENO_DB;

CREATE SCHEMA IF NOT EXISTS XENO_DB.CORE; --- Gold .GOLD

-- ------------------------------------------------------------
-- 1. Stage do S3 apontando pra pasta silver/ (arquivos parquet,
--    um por gravação: <recording_id>.parquet)
-- ------------------------------------------------------------
CREATE OR REPLACE STAGE XENO_DB.RAW.S3_STAGE_SILVER
    URL = 's3://xeno-canto-s3/silver/'
    CREDENTIALS = (
        AWS_KEY_ID = '<AWS_KEY_ID>'
        AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
        AWS_TOKEN = '<AWS_SESSION_TOKEN>'
    )
    FILE_FORMAT = (TYPE = PARQUET);

-- Sanity check: lista o que o Snowflake enxerga no bucket
-- (confirma que a credencial e o path estão corretos antes do COPY INTO)
LIST @XENO_DB.RAW.S3_STAGE_SILVER;

-- ------------------------------------------------------------
-- 2. Tabela silver: colunas tipadas direto (sem VARIANT), já
--    que o parquet chega estruturado. Schema espelha o parquet
--    real (56 colunas: id, taxonomia, lat/lon/alt, features
--    librosa mfcc 1-20 mean/std, rms, zcr, spectral, etc.)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS XENO_DB.RAW.SRC_RECORDING_FEATURES (
    id                          NUMBER,
    gen                         STRING,
    sp                          STRING,
    ssp                         STRING,
    en                          STRING,     -- nome popular
    status                      STRING,     -- status da identificação taxonômica
    cnt                         STRING,     -- país
    loc                         STRING,     -- localização textual
    q                           STRING,     -- rating de qualidade (A-E)
    url                         STRING,     -- URL da gravação no Xeno-canto
    date                        DATE,       -- data da gravação
    type                        STRING,     -- tipo de vocalização (song, call, alarm...)
    lat                         FLOAT,
    lon                         FLOAT,
    alt                         NUMBER,
    grp                         STRING,
    duration_seconds            FLOAT,
    energy_mean                 FLOAT,
    rms_mean                    FLOAT,
    rms_std                     FLOAT,
    zcr_mean                    FLOAT,
    zcr_std                     FLOAT,
    spectral_centroid_mean      FLOAT,
    spectral_bandwidth_mean     FLOAT,
    mfcc_1_mean FLOAT, mfcc_2_mean FLOAT, mfcc_3_mean FLOAT, mfcc_4_mean FLOAT,
    mfcc_5_mean FLOAT, mfcc_6_mean FLOAT, mfcc_7_mean FLOAT, mfcc_8_mean FLOAT,
    mfcc_9_mean FLOAT, mfcc_10_mean FLOAT, mfcc_11_mean FLOAT, mfcc_12_mean FLOAT,
    mfcc_13_mean FLOAT, mfcc_14_mean FLOAT, mfcc_15_mean FLOAT, mfcc_16_mean FLOAT,
    mfcc_17_mean FLOAT, mfcc_18_mean FLOAT, mfcc_19_mean FLOAT, mfcc_20_mean FLOAT,
    mfcc_1_std FLOAT, mfcc_2_std FLOAT, mfcc_3_std FLOAT, mfcc_4_std FLOAT,
    mfcc_5_std FLOAT, mfcc_6_std FLOAT, mfcc_7_std FLOAT, mfcc_8_std FLOAT,
    mfcc_9_std FLOAT, mfcc_10_std FLOAT, mfcc_11_std FLOAT, mfcc_12_std FLOAT,
    mfcc_13_std FLOAT, mfcc_14_std FLOAT, mfcc_15_std FLOAT, mfcc_16_std FLOAT,
    mfcc_17_std FLOAT, mfcc_18_std FLOAT, mfcc_19_std FLOAT, mfcc_20_std FLOAT,
    source_file_name            STRING,
    loaded_at                   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ------------------------------------------------------------
-- 3. Role read-only dedicada para o Metabase.
--    Acesso só à schema GOLD -- Metabase nunca precisa ver
--    RAW (dado bruto) nem STAGING (camada intermediária).
-- ------------------------------------------------------------
CREATE ROLE IF NOT EXISTS METABASE_RO;

GRANT USAGE ON WAREHOUSE XENO_WH TO ROLE METABASE_RO;
GRANT USAGE ON DATABASE XENO_DB TO ROLE METABASE_RO;
GRANT USAGE ON SCHEMA XENO_DB.CORE TO ROLE METABASE_RO;
GRANT SELECT ON ALL TABLES IN SCHEMA XENO_DB.CORE TO ROLE METABASE_RO;
GRANT SELECT ON FUTURE TABLES IN SCHEMA XENO_DB.CORE TO ROLE METABASE_RO;

-- Usuário de serviço que o Metabase vai usar na conexão
-- (troque a senha antes de rodar; use um PAT/senha forte)
CREATE USER IF NOT EXISTS METABASE_SVC
    PASSWORD = 'TROQUE_ESSA_SENHA'
    DEFAULT_ROLE = METABASE_RO
    DEFAULT_WAREHOUSE = XENO_WH
    MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE METABASE_RO TO USER METABASE_SVC;
