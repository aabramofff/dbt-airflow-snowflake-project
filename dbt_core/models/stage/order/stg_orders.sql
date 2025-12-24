{{ config(materialized='view') }}

SELECT
    O_ORDERKEY AS ORDER_ID,
    O_CUSTKEY AS CUSTOMER_ID,
    O_ORDERDATE AS ORDER_DATE,

    O_ORDERPRIORITY AS ORDER_PRIORITY,

    O_CLERK AS CLERK_NAME,
    O_ORDERSTATUS AS ORDER_STATUS,
    O_TOTALPRICE AS TOTAL_PRICE,
    O_COMMENT AS COMMENT,
    'TPCH_SF1' AS RECORD_SOURCE,
    md5(upper(trim(cast(O_ORDERKEY AS string)))) AS ORDER_HK,
    md5(upper(trim(cast(O_CUSTKEY AS string)))) AS CUSTOMER_HK,
    md5(concat_ws(
        '|',
        coalesce(cast(O_ORDERDATE AS string), ''),
        coalesce(O_ORDERPRIORITY, ''),
        coalesce(O_CLERK, '')
    )) AS ORDER_DETAILS_HASHDIFF,

    md5(concat_ws(
        '|',
        coalesce(O_ORDERSTATUS, ''),
        coalesce(cast(O_TOTALPRICE AS string), '')
    )) AS ORDER_STATUS_HASHDIFF,
    current_timestamp() AS LOAD_DATE

FROM {{ source('tpch_raw', 'orders') }}
