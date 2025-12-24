{{ config(materialized='table', tags=['marts']) }}

SELECT
    h.customer_hk AS customer_key,
    h.customer_id,
    s_inv.name AS customer_name,
    s_inv.market_segment,
    s_var.active_balance
FROM {{ ref('hub_customer') }} AS h
LEFT JOIN {{ ref('sat_customer_invar') }} AS s_inv
    ON h.customer_hk = s_inv.customer_hk
LEFT JOIN {{ ref('sat_customer_var') }} AS s_var
    ON h.customer_hk = s_var.customer_hk
