
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select crash_id
from "swans_demo"."stg"."stg_ca_crashes"
where crash_id is null



  
  
      
    ) dbt_internal_test