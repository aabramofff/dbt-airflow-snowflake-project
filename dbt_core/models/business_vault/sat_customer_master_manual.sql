{{ config( materialized='table' ) }}

WITH hub_data AS (
    SELECT
        customer_hk,
        customer_id
    FROM {{ ref('hub_customer') }}
),

seed_data AS (
    SELECT
        customer_id,
        segment_name,
        priority_score
    FROM {{ ref('manual_customer_segments') }}
)

SELECT
    h.customer_hk,
    s.segment_name,
    s.priority_score,
    'MANUAL_SEED' AS record_source,
    CURRENT_TIMESTAMP() AS load_date
FROM hub_data AS h
INNER JOIN seed_data AS s
    ON h.customer_id = CAST(s.customer_id AS NUMBER)
