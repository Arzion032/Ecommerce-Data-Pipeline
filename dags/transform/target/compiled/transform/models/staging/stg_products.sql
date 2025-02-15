SELECT 
    METADATA$FILENAME AS file_name,
    METADATA$FILE_ROW_NUMBER AS row_number,
    $1:"goods-title-link--jump"::STRING AS goods_title_link_jump, 
    $1:"goods-title-link--jump href"::STRING AS goods_title_link_href, 
    $1:"rank-title"::STRING AS rank_title,
    $1:"rank-sub"::STRING AS rank_sub,
    $1:"price"::STRING AS price,
    $1:"discount"::STRING AS discount,
    $1:"selling_proposition"::STRING AS selling_proposition,
    $1:"goods-title-link"::STRING AS goods_title_link,
    $1:"color-count"::INTEGER AS color_count,
    $1:"blackfridaybelts-bg src"::STRING AS blackfridaybelts_bg_src,
    $1:"blackfridaybelts-content"::STRING AS blackfridaybelts_content,
FROM 
    @product_stage
    (file_format => 'parquet_format', pattern => '.*.parquet')