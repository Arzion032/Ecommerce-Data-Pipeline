WITH clean_cols AS 
    (
    SELECT
        GOODS_TITLE_LINK_JUMP,
        SPLIT("FILE_NAME", '-')[2]::STRING AS category,
        SPLIT(LTRIM(RANK_TITLE, '#'), ' ')[0]::INTEGER AS rank_title,
        LTRIM(INITCAP(RANK_SUB), 'In ') as rank_sub,
        REGEXP_REPLACE(PRICE, '[$,]', '')::FLOAT AS "PRICE($)",
        (SUBSTRING(DISCOUNT, 2, LENGTH(DISCOUNT)-2)::INTEGER / 100)::FLOAT AS DISCOUNT,
        CASE
            WHEN LEFT(selling_proposition, LENGTH(selling_proposition) - 15) LIKE '%k%' 
                THEN LEFT(selling_proposition, LENGTH(selling_proposition) - 16)::FLOAT * 1000
            ELSE LEFT(selling_proposition, LENGTH(selling_proposition) - 15)::FLOAT
        END AS selling_proposition,
        COLOR_COUNT,
        LTRIM(BLACKFRIDAYBELTS_CONTENT, 'Save $')::FLOAT AS "BLACKFRIDAYBELTS_CONTENT(Save $)"
    FROM {{ ref('stg_products') }}
    ),
null_filled AS (
    SELECT
        GOODS_TITLE_LINK_JUMP,
        category,
        rank_title,
        rank_sub,
        ROUND(COALESCE("PRICE($)", COALESCE(AVG("PRICE($)") OVER(PARTITION BY category),0)) ,2) AS price_dollars,
        ROUND(COALESCE(DISCOUNT, COALESCE(AVG(DISCOUNT) OVER(PARTITION BY category),0)) ,2) AS discount,
        COALESCE(selling_proposition, COALESCE(AVG(selling_proposition) OVER(PARTITION BY category),0))::INTEGER AS selling_proposition,
        COLOR_COUNT,
        "BLACKFRIDAYBELTS_CONTENT(Save $)"
    FROM clean_cols
)


SELECT 
    * 
FROM null_filled
    