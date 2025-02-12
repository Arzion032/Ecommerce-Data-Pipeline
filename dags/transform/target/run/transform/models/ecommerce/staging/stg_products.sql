
  create or replace   view E_COMMERCE_PRODUCT.products_schema.stg_products
  
   as (
    SELECT 
    'goods-title-link--jump'::STRING, 
    "goods-title-link--jump href"::STRING, 
    "rank-title"::STRING,
    "rank-sub"::STRING,
    "price"::STRING,
    "discount"::STRING,
    "selling_proposition"::STRING,
    "goods-title-link"::STRING,
    "color-count"::INTEGER,
    "blackfridaybelts-bg src"::STRING,
    "blackfridaybelts-content"::STRING
FROM E_COMMERCE_PRODUCT.products_schema.products
  );

