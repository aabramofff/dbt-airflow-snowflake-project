{{ config(materialized='view') }}

SELECT
    h.order_hk,
    s.total_price,
    'BUSINESS_LOGIC' AS record_source,
    CURRENT_TIMESTAMP() AS load_date,
    ROUND(s.total_price * 0.15, 2) AS tax_amount,
    ROUND(s.total_price * 0.85, 2) AS net_amount
FROM {{ ref('hub_order') }} AS h
INNER JOIN {{ ref('sat_order_status') }} AS s
    ON h.order_hk = s.order_hk
