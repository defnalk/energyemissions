{% snapshot snap_installations %}

{{
    config(
      target_schema='staging',
      unique_key='installation_id',
      strategy='check',
      check_cols=['installation_name', 'country_code', 'sector'],
    )
}}

select
    installation_id,
    installation_name,
    country_code,
    sector,
    latitude,
    longitude
from {{ ref('stg_installations') }}

{% endsnapshot %}
