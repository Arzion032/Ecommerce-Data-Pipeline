WITH cat_perf AS
(
    SELECT
        ct.category AS category,
        SUM(fct_prod.price_dollars * fct_prod.selling_proposition) AS revenue,
        SUM(fct_prod.selling_proposition) AS total_units_sold,
        ROUND(AVG(fct_prod.price_dollars), 2) AS avg_price_dollars,
        ROUND(AVG(fct_prod.discount), 2) AS avg_discount,
        COUNT(*) AS total_products,
        CAST(
        CASE 
            WHEN SUM(fct_prod.selling_proposition) = 0 THEN 0  -- Returning 0 as a float
            ELSE SUM(fct_prod.price_dollars * fct_prod.selling_proposition) / SUM(selling_proposition)
        END
        AS FLOAT) AS avg_order_value
    FROM E_COMMERCE_PRODUCT.products_schema_analytics.fact_products_performance AS fct_prod
    JOIN E_COMMERCE_PRODUCT.products_schema_analytics.dim_category AS ct
    ON fct_prod.category_id = ct.category_id
    GROUP BY ct.category
)

SELECT 
    *       
FROM cat_perf
ORDER BY revenue DESC