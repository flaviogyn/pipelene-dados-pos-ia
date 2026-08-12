-- Rode este script UMA VEZ no Snowflake (na interface web, aba Worksheets)
-- para preparar o ambiente antes de subir o Airflow.

-- Warehouse (a "máquina" que processa) e database
CREATE OR REPLACE WAREHOUSE XENO_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 120  -- desliga sozinho após 60s parado (economiza créditos)
  AUTO_RESUME = true;

-- Database (a "pasta" que guarda os dados)
CREATE DATABASE IF NOT EXISTS XENO_DB;

-- Schemas 
-- RAW recebe os dados do S3
-- XENO_DB/STAGING são preenchidos pelo dbt
CREATE SCHEMA IF NOT EXISTS XENO_DB.RAW;      -- Bronze
CREATE SCHEMA IF NOT EXISTS XENO_DB.STAGING;  -- Silver
CREATE SCHEMA IF NOT EXISTS XENO_DB.CORE;     -- Gold
CREATE SCHEMA IF NOT EXISTS XENO_DB.GOLD;     -- Gold

-- 1. Stage externo apontando para a pasta bronze/ do bucket
--    Aponta para a pasta, não para um arquivo fixo, pois cada
--    espécie gera um arquivo bird-sp-<especie>.json diferente.
CREATE OR REPLACE STAGE XENO_DB.RAW.S3_STAGE
    URL = 's3://xeno-canto-s3/bronze/metadata/'
    CREDENTIALS = (
        AWS_KEY_ID = '<AWS_KEY_ID>'
        AWS_SECRET_KEY = '<AWS_SECRET_ACCESS_KEY>'
        AWS_TOKEN = '<AWS_SESSION_TOKEN>'
    )
    FILE_FORMAT = (
        TYPE = JSON
        STRIP_OUTER_ARRAY = TRUE
    )
    COMMENT = 'Stage de leitura da camada bronze no S3 (xeno-canto-s3/bronze/metadata)';

-- Sanity check: lista o que o Snowflake enxerga no bucket
-- (confirma que a credencial e o path estão corretos antes do COPY INTO)
LIST @XENO_DB.RAW.S3_STAGE;

-- 2. Tabela bronze: dado cru em VARIANT + metadados de carga.
--    Nenhum parsing de campo aqui -- isso é responsabilidade da
--    camada staging do dbt (stg_recordings.sql lendo raw_data:campo::tipo).
CREATE TABLE IF NOT EXISTS XENO_DB.RAW.SRC_RECORDINGS (
    raw_data     VARIANT,
    file_name    STRING,
    loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. (Opcional, recomendado) Load history para debug de cargas:
--    útil para conferir quais arquivos já foram processados pelo COPY INTO
--    e evitar dúvidas sobre reprocessamento.
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--     TABLE_NAME => 'XENO_DB.RAW.SRC_RECORDINGS',
--     START_TIME => DATEADD('hours', -24, CURRENT_TIMESTAMP())
-- ));
