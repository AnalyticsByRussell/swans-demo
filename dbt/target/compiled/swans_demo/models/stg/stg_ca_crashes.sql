-- models/stg/stg_ca_crashes.sql




with raw_data as (

    select *
    from "swans_demo"."raw"."ca_crashes_json_only"

)

select
    crash_id,
    -- Extract county code from JSON
    raw_json ->> 'County Code' as county_code,
    raw_json ->> 'County Name' as county_name,
    raw_json ->> 'City Name' as city_name,
    raw_json ->> 'PrimaryRoad' as primary_road,
    raw_json ->> 'Collision Type Description' as collision_type,
    raw_json ->> 'NumberKilled' as number_killed,
    raw_json ->> 'NumberInjured' as number_injured,
    raw_json ->> 'Crash Date Time' as crash_datetime
from raw_data