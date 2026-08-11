{{ config(materialized='table') }}

select
    id as usuario_id,
    first_name as nome,
    upper(country) as pais,
    created_at as data_cadastro
from {{ source('xeno_raw', 'users') }}