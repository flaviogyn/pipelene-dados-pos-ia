/*
    dim_species
    Uma linha por combinação (gênero, espécie, subespécie) observada
    nas gravações, com a flag de espécie-alvo do classificador binário.
    materialized/schema herdados de dbt_project.yml (marts: table, schema: gold)
*/

with species as (

    select distinct
        genus,
        species,
        subspecies,
        common_name
    from {{ ref('stg_recording_features') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['genus', 'species', 'coalesce(subspecies, \'\')']) }}
                                        as species_key,
    genus,
    species,
    subspecies,
    common_name,
    (genus = '{{ var("target_genus") }}'
        and species = '{{ var("target_species") }}')  as is_target_species

from species
