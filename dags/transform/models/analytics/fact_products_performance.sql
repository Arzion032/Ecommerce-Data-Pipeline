SELECT 
    sk_id AS prod_id,
    ct.category_id,
    price_dollars,
    discount,
    selling_proposition,
    blackfriday_savings
FROM {{ ref('int_products') }} AS inttable
JOIN {{ ref('agg_category_performance') }} AS ct