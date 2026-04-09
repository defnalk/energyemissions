with source as (
    select * from {{ source('raw', 'allowances') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['installation_id', 'year']) }} as allowance_id,
        installation_id,
        year,
        allocated_tonnes,
        surrendered_tonnes,
        _loaded_at as loaded_at
    from source
)

select * from renamed
