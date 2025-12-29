{{ config(
    materialized='incremental',
    tags=['business_vault']
) }}

WITH link_data AS (
    SELECT
        order_hk,
        customer_hk,
        load_date,
        record_source
    FROM {{ ref('link_order_customer') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_hk', 'customer_hk', 'load_date']) }} AS eff_sat_hk,
    order_hk,
    customer_hk,
    load_date AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    TRUE AS is_active,
    'BUSINESS_LOGIC' AS record_source
FROM link_data

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE
            t.order_hk = link_data.order_hk
            AND t.customer_hk = link_data.customer_hk
    )
{% endif %}
