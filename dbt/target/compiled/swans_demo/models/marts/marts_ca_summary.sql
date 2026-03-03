-- models/marts/marts_ca_summary.sql




with fct as (
    select *
    from "swans_demo"."fct"."fct_ca_crashes"
)

select
    county_name as county,
    count(*) as total_crashes,
    sum(number_killed) as total_killed,
    sum(number_injured) as total_injured
from fct
group by county_name
order by total_crashes desc