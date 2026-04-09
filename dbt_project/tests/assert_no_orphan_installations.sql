select e.installation_id
from {{ ref('stg_emissions') }} e
left join {{ ref('stg_installations') }} i using (installation_id)
where i.installation_id is null
