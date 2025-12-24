{{ config(materialized='view', tags=['business_vault']) }}

SELECT
    h.order_hk,
    s.total_price,
    ROUND(s.total_price * 0.15, 2) AS tax_amount,
    ROUND(s.total_price * 0.85, 2) AS net_amount,
    CURRENT_TIMESTAMP() AS load_date,
    'BUSINESS_LOGIC' AS record_source
FROM {{ ref('hub_order') }} h
JOIN {{ ref('sat_order_status') }} s ON h.order_hk = s.order_hk
