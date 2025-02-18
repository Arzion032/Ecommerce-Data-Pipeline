
  
    

        create or replace transient table E_COMMERCE_PRODUCT.products_schema_analytics.dim_products
         as
        (SELECT 
    inttable.sk_id AS prod_id,
    inttable.title,
    ct.category_id,
    inttable.rank_title,
    inttable.rank_sub,
    inttable.GOODS_TITLE_LINK_JUMP,
    inttable."COLOR_COUNTS",
    inttable.blackfriday_savings
FROM E_COMMERCE_PRODUCT.products_schema_intermediate.int_products AS inttable
JOIN E_COMMERCE_PRODUCT.products_schema_analytics.dim_category AS ct
    ON inttable.category = ct.category
        );
      
  