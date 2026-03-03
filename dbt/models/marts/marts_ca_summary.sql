-- models/marts/marts_ca_summary.sql
{{ config(schema='marts', materialized='view') }}

{{ config(
    materialized='table'
) }}

with fct as (
    select *
    from {{ ref('fct_ca_crashes') }}
)

select
    county_name as county,
    count(*) as total_crashes,
    sum(number_killed) as total_killed,
    sum(number_injured) as total_injured
from fct
group by county_name
order by total_crashes desc