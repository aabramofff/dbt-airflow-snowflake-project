{{ config(materialized='table') }}

SELECT
    h.order_hk,
    CURRENT_TIMESTAMP() AS as_of_date,
    MAX(sd.load_date) AS sat_order_details_ldts,
    MAX(ss.load_date) AS sat_order_status_ldts
FROM {{ ref('hub_order') }} AS h
LEFT JOIN {{ ref('sat_order_details') }} AS sd ON h.order_hk = sd.order_hk
LEFT JOIN {{ ref('sat_order_status') }} AS ss ON h.order_hk = ss.order_hk
GROUP BY 1, 2
