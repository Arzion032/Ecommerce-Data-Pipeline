WITH cat_desc AS
(
    SELECT 
        DISTINCT category
    FROM E_COMMERCE_PRODUCT.products_schema_intermediate.int_products
)

SELECT 
    md5(cast(coalesce(cast(category as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS category_id,
    category
FROM cat_desc