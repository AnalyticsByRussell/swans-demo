
  
    

  create  table "swans_demo"."marts"."ca_crashes_mart__dbt_tmp"
  
  
    as
  
  (
    -- models/marts/ca_crashes_mart.sql


select *
from "swans_demo"."marts"."int_ca_crashes"
  );
  