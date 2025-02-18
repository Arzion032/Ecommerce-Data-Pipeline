SELECT 
    inttable.sk_id AS prod_id,
    inttable.title,
    ct.category_id,
    inttable.rank_title,
    inttable.rank_sub,
    inttable.GOODS_TITLE_LINK_JUMP,
    inttable."COLOR_COUNTS",
    inttable.blackfriday_savings
FROM {{ ref('int_products') }} AS inttable
JOIN {{ ref('dim_category') }} AS ct
    ON inttable.category = ct.category

