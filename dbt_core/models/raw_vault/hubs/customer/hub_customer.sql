{{ config(materialized='incremental') }}

SELECT
    customer_hk,
    customer_id,
    load_date,
    record_source
FROM {{ ref('stg_customer') }}

{% if is_incremental() %}
    WHERE customer_hk NOT IN (SELECT customer_hk FROM {{ this }})
{% endif %}
