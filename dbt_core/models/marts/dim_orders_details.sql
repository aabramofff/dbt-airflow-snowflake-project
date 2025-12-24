{{ config(materialized='table', tags=['marts']) }}

SELECT
    h.order_hk AS order_key,
    h.order_id,
    s.order_date,
    s.clerk_name,
    s.order_priority
FROM {{ ref('hub_order') }} h
LEFT JOIN {{ ref('sat_order_details') }} s ON h.order_hk = s.order_hk