{{ config(
    schema='stg',
    materialized='view'
) }}

with raw_data as (
    select *
    from {{ source('raw', 'ca_crashes_json_only') }}
),

parsed as (
    select
        {{ get_collision_json_fields() }}
    from raw_data r
),

enhanced as (
    select
        p.*,
        f."Dataset County Name"     as county_name,
        f."Local County Name"       as county_region,
        f."Latitude"                as county_latitude,
        f."Longitude"               as county_longitude,
        case 
            when f."Dataset County Code" is null and p.county_code is not null then 1
            else 0
        end as unmatched_county_flag
    from parsed p
    left join {{ ref('ca_counties_lookup') }} f
        on p.county_code = f."Dataset County Code"::text
)

select *
from enhanced