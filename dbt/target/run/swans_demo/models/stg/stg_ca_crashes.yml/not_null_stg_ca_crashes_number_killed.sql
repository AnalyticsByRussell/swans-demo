
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select number_killed
from "swans_demo"."stg"."stg_ca_crashes"
where number_killed is null



  
  
      
    ) dbt_internal_test