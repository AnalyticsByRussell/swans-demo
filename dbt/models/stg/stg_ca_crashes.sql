-- models/stg/stg_ca_crashes.sql
{{ config(schema='stg', materialized='view') }}

with raw_data as (
    select *
    from {{ source('raw', 'ca_crashes_json_only') }}
)

select
    crash_id,
    raw_json ->> 'County Code' as county_code,
    raw_json ->> 'County Name' as county_name,
    raw_json ->> 'City Name' as city_name,
    raw_json ->> 'PrimaryRoad' as primary_road,
    raw_json ->> 'Collision Type Description' as collision_type,
    coalesce((raw_json ->> 'NumberKilled')::int, 0) as number_killed,
    coalesce((raw_json ->> 'NumberInjured')::int, 0) as number_injured,
    case
      when raw_json ->> 'Crash Date Time' ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$'
        then (raw_json ->> 'Crash Date Time')::timestamp
      else null
    end as crash_datetime
from raw_data