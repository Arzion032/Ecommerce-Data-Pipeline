SELECT sp.* 
FROM {{ ref('stg_products') }} AS sp