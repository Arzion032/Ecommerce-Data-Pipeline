WITH cols_to_keep AS
(
    SELECT 
        inttable.category AS category,
        ROUND(AVG(price_dollars), 2) AS avg_price_dollars,
        ROUND(AVG(discount), 2) AS avg_discount,
        ROUND(AVG(selling_proposition), 2) AS avg_sold_proposition,
        COUNT(*) AS total_products,
    FROM {{ ref('int_products') }} AS inttable
    JOIN {{ ref('dim_category')}} AS ct
    ON inttable.category = ct.category
    GROUP BY inttable.category
    ORDER BY avg_price_dollars DESC
),

id_generated AS 
(
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['category']) }} AS category_id,
        avg_price_dollars,
        avg_discount,
        avg_sold_proposition
    FROM cols_to_keep
)

SELECT 
    *       
FROM id_generated