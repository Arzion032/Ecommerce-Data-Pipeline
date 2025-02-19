select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select discount
from E_COMMERCE_PRODUCT.products_schema_intermediate.int_products
where discount is null



      
    ) dbt_internal_test