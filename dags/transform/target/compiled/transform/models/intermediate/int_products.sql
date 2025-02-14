WITH clean_cols AS 
    (
    SELECT
        GOODS_TITLE_LINK_JUMP,
        SPLIT("FILE_NAME", '-')[2] AS "category",
        SPLIT(LTRIM(RANK_TITLE, '#'), ' ')[0]::INTEGER AS rank_title,
        LTRIM(INITCAP(RANK_SUB), 'In ') as rank_sub,
        PRICE,
        DISCOUNT,
        CASE
            WHEN LEFT(selling_proposition, LENGTH(selling_proposition) - 15) LIKE '%k%' 
                THEN LEFT(selling_proposition, LENGTH(selling_proposition) - 16)::FLOAT * 1000
            ELSE LEFT(selling_proposition, LENGTH(selling_proposition) - 15)::FLOAT
        END AS selling,
        selling_proposition,
        COLOR_COUNT,
        BLACKFRIDAYBELTS_CONTENT
    FROM E_COMMERCE_PRODUCT.products_schema.stg_products
    )


SELECT 
    DISTINCT * 
FROM clean_cols