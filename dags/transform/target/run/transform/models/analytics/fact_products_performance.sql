
  
    

        create or replace transient table E_COMMERCE_PRODUCT.products_schema_analytics.fact_products_performance
         as
        (SELECT 
    prod.prod_id,
    prod.category_id,
    inttable.price_dollars,
    inttable.discount,
    inttable.selling_proposition,
    inttable.blackfriday_savings
FROM E_COMMERCE_PRODUCT.products_schema_analytics.dim_products AS prod
JOIN E_COMMERCE_PRODUCT.products_schema_intermediate.int_products AS inttable
ON prod.prod_id = inttable.sk_id
        );
      
  