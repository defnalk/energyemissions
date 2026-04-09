select *
from {{ ref('stg_emissions') }}
where verified_tonnes < 0
