with source as (
    select * from {{ source('raw', 'installations') }}
),

renamed as (
    select
        installation_id,
        name as installation_name,
        country_code,
        sector,
        latitude,
        longitude,
        _loaded_at as loaded_at
    from source
)

select * from renamed
