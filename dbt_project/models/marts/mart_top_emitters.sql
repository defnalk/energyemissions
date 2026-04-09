with joined as (
    select * from {{ ref('int_emissions_joined') }}
),

ranked as (
    select
        installation_id,
        installation_name,
        country_code,
        sector,
        year,
        sum(verified_tonnes) as total_emissions_tonnes,
        row_number() over (partition by year order by sum(verified_tonnes) desc) as rank_in_year
    from joined
    group by 1, 2, 3, 4, 5
)

select * from ranked where rank_in_year <= 100
