{{ config(materialized='view') }}

SELECT
    C_CUSTKEY AS CUSTOMER_ID,
    C_NAME AS NAME,

    C_MKTSEGMENT AS MARKET_SEGMENT,

    C_NATIONKEY AS NATION_KEY,
    C_ADDRESS AS ADDRESS,
    C_PHONE AS PHONE,
    C_ACCTBAL AS ACTIVE_BALANCE,
    C_COMMENT AS COMMENT,
    'TPCH_SF1' AS RECORD_SOURCE,
    md5(upper(trim(cast(C_CUSTKEY AS string)))) AS CUSTOMER_HK,
    md5(concat_ws(
        '|',
        coalesce(C_NAME, ''),
        coalesce(C_MKTSEGMENT, ''),
        coalesce(cast(C_NATIONKEY AS string), '')
    )) AS CUSTOMER_INVAR_HASHDIFF,
    md5(concat_ws(
        '|',
        coalesce(C_ADDRESS, ''),
        coalesce(C_PHONE, ''),
        coalesce(cast(C_ACCTBAL AS string), ''),
        coalesce(C_COMMENT, '')
    )) AS CUSTOMER_VAR_HASHDIFF,
    current_timestamp() AS LOAD_DATE
FROM {{ source('tpch_raw', 'customer') }}
