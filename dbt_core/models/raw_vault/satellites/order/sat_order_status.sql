{{ config(materialized='incremental') }}

SELECT
    order_hk,
    order_status_hashdiff AS hashdiff,
    order_status,
    total_price,
    load_date,
    record_source
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
    WHERE
        hashdiff NOT IN (
            SELECT hashdiff FROM {{ this }}
            WHERE order_hk = order_hk
        )
{% endif %}
