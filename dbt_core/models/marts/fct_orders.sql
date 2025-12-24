{{ config(materialized='table', tags=['marts']) }}

SELECT
    pit.order_hk AS order_key,
    loc.customer_hk AS customer_key,
    sos.total_price,
    sos.order_status,
    som.tax_amount,
    som.net_amount
FROM {{ ref('pit_order') }} pit
JOIN {{ ref('link_order_customer') }} loc ON pit.order_hk = loc.order_hk
JOIN {{ ref('sat_order_status') }} sos 
    ON pit.order_hk = sos.order_hk AND pit.sat_order_status_ldts = sos.load_date
JOIN {{ ref('sat_order_metrics') }} som ON pit.order_hk = som.order_hk
