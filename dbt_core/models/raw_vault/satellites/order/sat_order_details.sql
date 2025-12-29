{{ config(materialized='incremental') }}

SELECT
    order_hk,
    order_details_hashdiff AS hashdiff,
    order_date,
    order_priority,
    clerk_name,
    comment,
    load_date,
    record_source
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
    WHERE
        hashdiff NOT IN (SELECT hashdiff FROM {{ this }})
        AND load_date >= (SELECT MAX(load_date) FROM {{ this }})
{% endif %}
