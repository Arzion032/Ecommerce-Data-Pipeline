select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price_dollars
from E_COMMERCE_PRODUCT.products_schema_intermediate.int_products
where price_dollars is null



      
    ) dbt_internal_test