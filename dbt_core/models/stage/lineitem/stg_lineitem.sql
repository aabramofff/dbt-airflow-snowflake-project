{{ config(materialized='view') }}

SELECT
    L_ORDERKEY AS ORDER_ID,
    L_LINENUMBER AS LINE_NUMBER,
    L_PARTKEY AS PART_ID,
    L_QUANTITY AS QUANTITY,
    L_EXTENDEDPRICE AS EXTENDED_PRICE,
    'TPCH_SF1' AS RECORD_SOURCE,
    md5(upper(trim(cast(L_ORDERKEY AS string)))) AS ORDER_HK,
    md5(upper(trim(cast(concat(L_ORDERKEY, '-', L_LINENUMBER) AS string))))
        AS LINEITEM_HK,
    md5(upper(trim(cast(L_PARTKEY AS string)))) AS PART_HK,
    current_timestamp() AS LOAD_DATE
FROM {{ source('tpch_raw', 'lineitem') }}
