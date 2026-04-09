with emissions as (
    select * from {{ ref('stg_emissions') }}
),

installations as (
    select * from {{ ref('stg_installations') }}
),

countries as (
    select * from {{ source('raw', 'country_codes') }}
)

select
    e.emission_id,
    e.installation_id,
    i.installation_name,
    i.sector,
    i.country_code,
    c.country_name,
    c.population,
    e.year,
    e.activity_type,
    e.verified_tonnes
from emissions e
inner join installations i using (installation_id)
left join countries c using (country_code)
