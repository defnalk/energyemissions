select *
from {{ ref('stg_emissions') }}
where year < 2005 or year > extract(year from current_date)::int + 1
