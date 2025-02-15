
  
    

        create or replace transient table E_COMMERCE_PRODUCT.products_schema_analytics.agg_category_performance
         as
        (WITH cols_to_keep AS
(
    SELECT 
        inttable.category AS category,
        ROUND(AVG(price_dollars), 2) AS avg_price_dollars,
        ROUND(AVG(discount), 2) AS avg_discount,
        ROUND(AVG(selling_proposition), 2) AS avg_sold_proposition,
        COUNT(*) AS total_products,
    FROM E_COMMERCE_PRODUCT.products_schema_intermediate.int_products AS inttable
    JOIN E_COMMERCE_PRODUCT.products_schema_analytics.dim_category AS ct
    ON inttable.category = ct.category
    GROUP BY inttable.category
    ORDER BY avg_price_dollars DESC
),

id_generated AS 
(
    SELECT 
        md5(cast(coalesce(cast(category as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS category_id,
        avg_price_dollars,
        avg_discount,
        avg_sold_proposition
    FROM cols_to_keep
)

SELECT 
    *       
FROM id_generated
        );
      
  