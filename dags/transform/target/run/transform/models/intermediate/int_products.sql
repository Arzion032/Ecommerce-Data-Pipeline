
  
    

        create or replace transient table E_COMMERCE_PRODUCT.products_schema_intermediate.int_products
         as
        (SELECT sp.* 
FROM E_COMMERCE_PRODUCT.products_schema.stg_products AS sp
        );
      
  