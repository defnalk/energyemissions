with joined as (
    select * from {{ ref('int_emissions_joined') }}
),

aggregated as (
    select
        country_code,
        country_name,
        population,
        year,
        sum(verified_tonnes) as total_emissions_tonnes,
        count(distinct installation_id) as installation_count
    from joined
    group by 1, 2, 3, 4
),

with_yoy as (
    select
        *,
        lag(total_emissions_tonnes) over (partition by country_code order by year) as prev_year_emissions,
        {{ safe_divide('total_emissions_tonnes', 'population') }} as emissions_per_capita
    from aggregated
)

select
    country_code,
    country_name,
    population,
    year,
    total_emissions_tonnes,
    installation_count,
    emissions_per_capita,
    {{ safe_divide('(total_emissions_tonnes - prev_year_emissions) * 100.0', 'prev_year_emissions') }} as yoy_pct_change
from with_yoy
