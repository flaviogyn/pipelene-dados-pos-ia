{{ config(materialized='view') }}

select
    id as usuario_id,
    first_name as nome,
    upper(country) as pais,
    created_at as data_cadastro
from {{ source('raw_data', 'users') }} -- Referência à sua tabela bruta real
