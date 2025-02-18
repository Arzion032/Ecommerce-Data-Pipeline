SELECT 
    prod.prod_id,
    prod.category_id,
    inttable.price_dollars,
    inttable.discount,
    inttable.selling_proposition,
    inttable.blackfriday_savings
FROM {{ ref('dim_products') }} AS prod
JOIN {{ ref('int_products') }} AS inttable
ON prod.prod_id = inttable.sk_id