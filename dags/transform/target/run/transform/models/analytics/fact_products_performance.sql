
  
    

        create or replace transient table E_COMMERCE_PRODUCT.products_schema_analytics.fact_products_performance
         as
        (SELECT 
    sk_id AS prod_id,
    ct.category_id,
    price_dollars,
    discount,
    selling_proposition,
    blackfriday_savings
FROM E_COMMERCE_PRODUCT.products_schema_intermediate.int_products AS inttable
JOIN E_COMMERCE_PRODUCT.products_schema_analytics.agg_category_performance AS ct
        );
      
  