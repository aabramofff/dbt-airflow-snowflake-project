{{ config(materialized='view', tags=['business_vault']) }}

SELECT
    h.order_hk,
    s.total_price,
    CURRENT_TIMESTAMP() AS load_date,
    'BUSINESS_LOGIC' AS record_source,
    ROUND(s.total_price * 0.15, 2) AS tax_amount,
    ROUND(s.total_price * 0.85, 2) AS net_amount
FROM {{ ref('hub_order') }} AS h
INNER JOIN {{ ref('sat_order_status') }} AS s
    ON h.order_hk = s.order_hk
    