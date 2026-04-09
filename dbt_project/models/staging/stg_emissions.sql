with source as (
    select * from {{ source('raw', 'emissions') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['installation_id', 'year', 'activity_type']) }} as emission_id,
        installation_id,
        year,
        activity_type,
        verified_tonnes,
        reporting_date,
        _loaded_at as loaded_at
    from source
)

select * from renamed
