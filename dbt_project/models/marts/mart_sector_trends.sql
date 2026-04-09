with joined as (
    select * from {{ ref('int_emissions_joined') }}
)

select
    sector,
    country_code,
    year,
    sum(verified_tonnes) as total_emissions_tonnes,
    count(distinct installation_id) as installation_count
from joined
group by 1, 2, 3
