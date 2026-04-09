with allowances as (
    select * from {{ ref('int_allowances_net') }}
),

emissions as (
    select
        installation_id,
        year,
        sum(verified_tonnes) as verified_tonnes
    from {{ ref('stg_emissions') }}
    group by 1, 2
)

select
    a.country_code,
    a.year,
    sum(a.allocated_tonnes) as allocated_tonnes,
    sum(a.surrendered_tonnes) as surrendered_tonnes,
    sum(coalesce(e.verified_tonnes, 0)) as verified_tonnes,
    sum(a.allocated_tonnes) - sum(coalesce(e.verified_tonnes, 0)) as compliance_gap_tonnes
from allowances a
left join emissions e using (installation_id, year)
group by 1, 2
