{{ config(materialized='incremental') }}

SELECT
    customer_hk,
    hashdiff,
    name,
    market_segment,
    nation_key,
    load_date,
    record_source
FROM (
    SELECT
        customer_hk,
        customer_invar_hashdiff AS hashdiff,
        name,
        market_segment,
        nation_key,
        load_date,
        record_source
    FROM {{ ref('stg_customer') }}
)
{% if is_incremental() %}
    WHERE
        hashdiff NOT IN (
            SELECT hashdiff FROM {{ this }}
            WHERE customer_hk = customer_hk
        )
{% endif %}
