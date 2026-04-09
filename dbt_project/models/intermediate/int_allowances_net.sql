with allowances as (
    select * from {{ ref('stg_allowances') }}
),

installations as (
    select * from {{ ref('stg_installations') }}
)

select
    a.allowance_id,
    a.installation_id,
    i.country_code,
    i.sector,
    a.year,
    a.allocated_tonnes,
    a.surrendered_tonnes,
    a.allocated_tonnes - a.surrendered_tonnes as net_position
from allowances a
inner join installations i using (installation_id)
