-- ============================================================
-- Setup da camada Bronze: Stage S3 + tabela STG_RECORDINGS
-- Executar uma única vez (ou sempre que credenciais/paths mudarem)
-- Rodar como ACCOUNTADMIN ou role com privilégio CREATE STAGE em XENO_DB.RAW
-- ============================================================

USE ROLE TRAINING_ROLE;
USE WAREHOUSE XENO_WH;
USE DATABASE XENO_DB;
USE SCHEMA RAW;

-- Schema separada para os modelos de staging do dbt (views).
-- Evita colisão de nomes com a tabela bronze SRC_RECORDINGS
-- e mantém RAW = dado bruto, STAGING = dado tipado/parseado.
CREATE SCHEMA IF NOT EXISTS XENO_DB.STAGING;

-- 1. Stage externo apontando para a pasta bronze/ do bucket
--    Aponta para a pasta, não para um arquivo fixo, pois cada
--    espécie gera um arquivo bird-sp-<especie>.json diferente.
CREATE OR REPLACE STAGE XENO_DB.RAW.S3_STAGE
    URL = 's3://xeno-canto-s3/bronze/'
    CREDENTIALS = (
        AWS_KEY_ID = 'SUA_KEY'
        AWS_SECRET_KEY = 'SEU_SECRET'
    )
    FILE_FORMAT = (
        TYPE = JSON
        STRIP_OUTER_ARRAY = TRUE
    )
    COMMENT = 'Stage de leitura da camada bronze no S3 (xeno-canto-s3/bronze/)';

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
