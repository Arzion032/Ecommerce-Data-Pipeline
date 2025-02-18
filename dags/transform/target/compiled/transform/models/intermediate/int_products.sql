WITH clean_cols AS 
    (
    SELECT
        goods_title_link,
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
    FROM E_COMMERCE_PRODUCT.products_schema.stg_products
    ),

null_filled AS (
    SELECT
        LOWER(TRIM(COALESCE(goods_title_link, 'Unknown'))) AS title,
        LOWER(TRIM(COALESCE(GOODS_TITLE_LINK_JUMP, 'No Link Available'))) AS goods_title_link_jump,  
        category,
        COALESCE(rank_title, -1) AS rank_title, 
        COALESCE(rank_sub, 'Unknown') AS rank_sub,
        ROUND(COALESCE("PRICE($)", AVG("PRICE($)") OVER(PARTITION BY category)),2) AS price_dollars,
        ROUND(COALESCE(DISCOUNT, AVG(DISCOUNT) OVER(PARTITION BY category)),2) AS discount,
        ROUND(COALESCE(selling_proposition, AVG(selling_proposition) OVER(PARTITION BY category),0),2)::INTEGER AS selling_proposition,
        COALESCE(COLOR_COUNT, 0) AS "COLOR_COUNTS",
        COALESCE("BLACKFRIDAYBELTS_CONTENT(Save $)", 0) AS blackfriday_savings
    FROM clean_cols
),

unique_rows AS (
SELECT 
    DISTINCT *
FROM null_filled
)

SELECT 
    md5(cast(coalesce(cast(discount as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(price_dollars as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(title as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(selling_proposition as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(category as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(rank_sub as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(rank_title as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast("COLOR_COUNTS" as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS sk_id,
     * 
FROM unique_rows