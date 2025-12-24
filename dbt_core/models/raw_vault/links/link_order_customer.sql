{{ config(materialized='incremental') }}

SELECT
    order_hk,
    customer_hk,
    load_date,
    record_source,
    md5(concat(order_hk, customer_hk)) AS link_order_customer_hk
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE
        md5(concat(order_hk, customer_hk)) NOT IN (
            SELECT link_order_customer_hk FROM {{ this }}
        )
{% endif %}
