
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select number_injured
from "swans_demo"."stg"."stg_ca_crashes"
where number_injured is null



  
  
      
    ) dbt_internal_test