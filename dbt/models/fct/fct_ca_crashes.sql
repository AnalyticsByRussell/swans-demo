-- models/fct/fct_ca_crashes.sql
{{ config(schema='fct', materialized='view') }}

{{ config(
    materialized='table'
) }}

with stg as (
    select *
    from {{ ref('stg_ca_crashes') }}
)

select
    crash_id,
    county_code,
    county_name,
    city_name,
    primary_road,
    collision_type,
    cast(number_killed as integer) as number_killed,
    cast(number_injured as integer) as number_injured,
    crash_datetime::timestamp as crash_datetime
from stg