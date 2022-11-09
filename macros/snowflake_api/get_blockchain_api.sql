{% macro get_blockchain_api() %}
  {% set query %}
  CREATE TABLE if NOT EXISTS bronze_api.blockchain(
    block_ids ARRAY,
    call STRING,
    DATA variant,
    _inserted_timestamp timestamp_ntz
  );
{% endset %}
  {% do run_query(query) %}
  {% set query %}
INSERT INTO
  bronze_api.blockchain(
    block_ids,
    call,
    DATA,
    _inserted_timestamp
  ) WITH base AS (
    SELECT
      MIN(block_id) min_block,
      MAX(block_id) max_block,
      ARRAY_AGG(block_id) blocks
    FROM
      (
        SELECT
          conditional_true_event(
            CASE
              WHEN rn_mod = 1 THEN TRUE
              ELSE FALSE
            END
          ) over (
            ORDER BY
              block_ID
          ) groupID,
          block_id
        FROM
          (
            SELECT
              block_Id,
              MOD(ROW_NUMBER() over(
            ORDER BY
              block_id), 20) rn_mod
            FROM
              (
                SELECT
                  DISTINCT block_id
                FROM
                  bronze.blocks
                WHERE
                  block_id > 6300426
                EXCEPT
                SELECT
                  VALUE
                FROM
                  bronze_api.blockchain,
                  LATERAL FLATTEN(block_ids)
              )
            ORDER BY
              1
          )
      )
    GROUP BY
      groupID
    LIMIT
      50
  )
SELECT
  blocks,
  CONCAT(
    '{ "jsonrpc": "2.0", "id": 1, "method": "blockchain", "params": ["',
    min_block,
    '","',
    max_block,
    '"] }'
  ) AS call,
  ethereum.streamline.udf_api(
    'POST',
    'https://osmosis-coke.allthatnode.com:56657',{ 'Authorization':(
      SELECT
        key
      FROM
        osmosis._internal.api_keys
      WHERE
        provider = 'allthatnode'
    ) },
    PARSE_JSON(call)
  ) AS resp,
  SYSDATE()
FROM
  base;
{% endset %}
  {% do run_query(query) %}
{% endmacro %}
