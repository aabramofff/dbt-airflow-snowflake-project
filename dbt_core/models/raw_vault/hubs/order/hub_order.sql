{{ config(materialized='incremental') }}

SELECT
    order_hk,
    order_id,
    load_date,
    record_source
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE order_hk NOT IN (SELECT order_hk FROM {{ this }})
{% endif %}
