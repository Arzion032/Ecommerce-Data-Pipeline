WITH cat_desc AS
(
    SELECT 
        DISTINCT category
    FROM {{ ref('int_products')}}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['category']) }} AS category_id,
    category
FROM cat_desc