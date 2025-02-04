{% macro get_blockchain_api() %}
  {% set query %}
  CREATE schema if NOT EXISTS {{ target.database }}.bronze_api;
{% endset %}
  {% do run_query(query) %}
  {% set query %}
  CREATE TABLE if NOT EXISTS {{ target.database }}.bronze_api.blockchain(
    call ARRAY,
    DATA variant,
    _inserted_timestamp timestamp_ntz
  );
{% endset %}
  {% do run_query(query) %}
  {% set query %}
INSERT INTO
  {{ target.database }}.bronze_api.blockchain(
    call,
    DATA,
    _inserted_timestamp
  ) WITH base AS (
    SELECT
      *
    FROM
      (
        SELECT
          *,
          conditional_true_event(
            CASE
              WHEN rn_mod_out = 1 THEN TRUE
              ELSE FALSE
            END
          ) over (
            ORDER BY
              min_block DESC
          ) groupID_out
        FROM
          (
            SELECT
              *,
              MOD(ROW_NUMBER() over(
            ORDER BY
              min_block), 1) rn_mod_out
            FROM
              (
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
                          block_Id :: STRING block_Id,
                          MOD(ROW_NUMBER() over(
                        ORDER BY
                          block_id), 20) rn_mod
                        FROM
                          (
                            SELECT
                              DISTINCT block_id
                            FROM
                              {{ target.database }}.silver.blocks
                            EXCEPT
                            SELECT
                              block_id
                            FROM
                              {{ target.database }}.silver.blockchain
                          )
                        ORDER BY
                          block_id DESC
                      )
                  )
                GROUP BY
                  groupID
              )
          )
      )
    WHERE
      groupID_out < 11
  ),
  calls AS (
    SELECT
      min_block,
      ARRAY_AGG(
        { 'jsonrpc': '2.0',
        'id': min_block :: INT,
        'method': 'blockchain',
        'params': [min_block::STRING,max_block::STRING] }
      ) call
    FROM
      base
    GROUP BY
      min_block
    LIMIT
      50
  )
SELECT
  call,
  {{ target.database }}.live.udf_api (
    'POST',
    '{Service}/{Authentication}',
    OBJECT_CONSTRUCT(
      'Content-Type',
      'application/json'
    ),
    call,
    'Vault/prod/osmosis/quicknode/mainnet_new'
  ) AS DATA,
  SYSDATE()
FROM
  calls;
{% endset %}
  {% do run_query(query) %}
{% endmacro %}
