{{ config(materialized='table') }}

select
    usuario_id,
    nome,
    pais
from {{ ref('stg_usuarios') }}
where data_cadastro >= '2026-01-01'
