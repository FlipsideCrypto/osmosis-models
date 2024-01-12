{{ config(
  materialized = 'incremental',
  full_refresh = false,
  tags = ['daily']
) }}

WITH call_1 AS (

  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools',{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
),
call_2 AS (
  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools?pagination.key=' || resp :data :pagination :next_key,{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
  FROM
    call_1
),
call_3 AS (
  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools?pagination.key=' || resp :data :pagination :next_key,{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
  FROM
    call_2
  WHERE
    resp :data :pagination :next_key <> 'null'
),
call_4 AS (
  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools?pagination.key=' || resp :data :pagination :next_key,{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
  FROM
    call_3
  WHERE
    resp :data :pagination :next_key <> 'null'
),
call_5 AS (
  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools?pagination.key=' || resp :data :pagination :next_key,{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
  FROM
    call_4
  WHERE
    resp :data :pagination :next_key <> 'null'
),
call_6 AS (
  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools?pagination.key=' || resp :data :pagination :next_key,{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
  FROM
    call_5
  WHERE
    resp :data :pagination :next_key <> 'null'
),
call_7 AS (
  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools?pagination.key=' || resp :data :pagination :next_key,{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
  FROM
    call_6
  WHERE
    resp :data :pagination :next_key <> 'null'
),
call_8 AS (
  SELECT
    live.udf_api(
      'GET',
      'https://lcd.osmosis.zone/osmosis/concentratedliquidity/v1beta1/pools?pagination.key=' || resp :data :pagination :next_key,{},{}
    ) AS resp,
    SYSDATE() AS _inserted_timestamp
  FROM
    call_7
  WHERE
    resp :data :pagination :next_key <> 'null'
),
fin AS (
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_1
  UNION ALL
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_2
  UNION ALL
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_3 {# WHERE
    resp IS NOT NULL #}
  UNION ALL
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_4 {# WHERE
    resp IS NOT NULL #}
  UNION ALL
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_5 {# WHERE
    resp IS NOT NULL #}
  UNION ALL
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_6 {# WHERE
    resp IS NOT NULL #}
  UNION ALL
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_7 {# WHERE
    resp IS NOT NULL #}
  UNION ALL
  SELECT
    resp,
    _inserted_timestamp
  FROM
    call_8 {# WHERE
    resp IS NOT NULL #}
)
SELECT
  resp,
  _inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  fin
