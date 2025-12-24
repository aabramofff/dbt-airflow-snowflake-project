{{ config(materialized='table') }}

SELECT
    pit.order_hk AS order_key,
    loc.customer_hk AS customer_key,
    sos.total_price,
    sos.order_status,
    som.tax_amount,
    som.net_amount
FROM {{ ref('pit_order') }} AS pit
INNER JOIN {{ ref('link_order_customer') }} AS loc
    ON pit.order_hk = loc.order_hk
INNER JOIN {{ ref('sat_order_status') }} AS sos
    ON
        pit.order_hk = sos.order_hk
        AND pit.sat_order_status_ldts = sos.load_date
INNER JOIN {{ ref('sat_order_metrics') }} AS som
    ON pit.order_hk = som.order_hk
