{{ config(materialized='table') }}

WITH pit AS (
    SELECT * FROM {{ ref('pit_order') }}
),

order_customer_link AS (
    SELECT
        order_hk,
        customer_hk
    FROM {{ ref('link_order_customer') }}
)

SELECT
    pit.order_hk AS order_key,
    loc.customer_hk AS customer_key,
    ss.total_price,
    ss.order_status,
    sd.order_priority,
    sd.clerk_name,
    CAST(sd.order_date AS DATE) AS order_date_key
FROM pit
LEFT JOIN order_customer_link AS loc
    ON pit.order_hk = loc.order_hk
LEFT JOIN {{ ref('sat_order_details') }} AS sd
    ON
        pit.order_hk = sd.order_hk
        AND pit.sat_order_details_ldts = sd.load_date
LEFT JOIN {{ ref('sat_order_status') }} AS ss
    ON
        pit.order_hk = ss.order_hk
        AND pit.sat_order_status_ldts = ss.load_date
