select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    goods_title_link_jump as unique_field,
    count(*) as n_records

from E_COMMERCE_PRODUCT.products_schema_intermediate.int_products
where goods_title_link_jump is not null
group by goods_title_link_jump
having count(*) > 1



      
    ) dbt_internal_test