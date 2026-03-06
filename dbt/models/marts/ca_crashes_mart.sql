-- models/marts/ca_crashes_mart.sql
{{ config(
    materialized='table'
) }}

select *
from {{ ref('int_ca_crashes') }}