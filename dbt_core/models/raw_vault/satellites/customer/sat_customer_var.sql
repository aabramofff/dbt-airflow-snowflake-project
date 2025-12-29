{{ config(materialized='incremental') }}

SELECT
    customer_hk,
    customer_var_hashdiff AS hashdiff,
    address,
    phone,
    active_balance,
    comment,
    load_date,
    record_source
FROM {{ ref('stg_customer') }}
{% if is_incremental() %}
    WHERE
        hashdiff NOT IN (
            SELECT hashdiff FROM {{ this }}
        )
        AND load_date > (SELECT MAX(load_date) FROM {{ this }})
{% endif %}
