
  create view "swans_demo"."dummy_stg"."stg_ca_crashes__dbt_tmp"
    
    
  as (
    -- models/stg/stg_ca_crashes.sql


with raw_data as (
    select *
    from "swans_demo"."raw"."ca_crashes_json_only"
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
    to_timestamp(raw_json ->> 'Crash Date Time', 'YYYY-MM-DD HH24:MI:SS') as crash_datetime
from raw_data
  );