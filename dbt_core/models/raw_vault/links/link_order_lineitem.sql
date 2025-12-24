{{ config(materialized='incremental') }}

SELECT
    order_hk,
    lineitem_hk,
    load_date,
    record_source,
    md5(concat(order_hk, lineitem_hk)) AS link_order_lineitem_hk
FROM {{ ref('stg_lineitem') }}

{% if is_incremental() %}
    WHERE
        md5(concat(order_hk, lineitem_hk)) NOT IN (
            SELECT link_order_lineitem_hk FROM {{ this }}
        )
{% endif %}
