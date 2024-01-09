{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, _body_index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['noncore']
) }}

WITH

{% if is_incremental() %}
max_date AS (

    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

swaps AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        tx_body,
        _inserted_timestamp,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                _inserted_timestamp ASC
        ) - 1 AS rn
    FROM
        {{ ref('silver__transactions') }}
        t,
        LATERAL FLATTEN (
            input => tx_body :messages,
            recursive => TRUE
        ) b
    WHERE
        key = '@type'
        AND VALUE :: STRING = '/osmosis.gamm.v1beta1.MsgSwapExactAmountIn'
        AND tx_succeeded = TRUE

{% if is_incremental() %}
AND t._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
msg_atts AS (
    SELECT
        tx_id,
        msg_group,
        msg_index,
        msg_type,
        attribute_key,
        attribute_value
    FROM
        {{ ref('silver__msg_attributes') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            max_date
    )
{% endif %}
),
pre_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_succeeded,
        b.value,
        b.value :sender :: STRING AS trader,
        COALESCE(
            b.value :tokenOutMinAmount :: NUMBER,
            b.value :token_out_min_amount :: NUMBER
        ) AS to_amount,
        b.value :token_in :amount :: NUMBER from_amount,
        b.value :token_in :denom :: STRING from_currency,
        b.value :routes AS routes,
        _inserted_timestamp,
        b.index AS _body_index
    FROM
        swaps s,
        TABLE(FLATTEN(tx_body :messages)) b
    WHERE
        b.value :routes IS NOT NULL
        AND b.index = rn
),
pools AS (
    SELECT
        tx_id,
        _body_index,
        ARRAY_AGG(
            COALESCE(
                r.value :poolId,
                r.value :pool_id
            )
        ) AS pool_ids
    FROM
        pre_final p,
        TABLE(FLATTEN(routes)) r
    GROUP BY
        tx_id,
        _body_index),
        msg_idx AS (
            SELECT
                p.tx_id,
                msg_group,
                MIN(
                    m.msg_index
                ) AS min_msg_index
            FROM
                pre_final p
                INNER JOIN msg_atts m
                ON p.tx_id = m.tx_id
            WHERE
                (
                    (
                        msg_type = 'token_swapped'
                        AND attribute_key = 'tokens_in'
                    )
                    OR (
                        msg_type = 'transfer'
                        AND attribute_key = 'amount'
                    )
                )
                AND msg_group IS NOT NULL
            GROUP BY
                p.tx_id,
                msg_group
        ),
        from_amt AS (
            SELECT
                m.tx_id,
                p.msg_index,
                m.msg_group,
                RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS from_currency,
                SPLIT_PART(
                    TRIM(
                        REGEXP_REPLACE(
                            attribute_value,
                            '[^[:digit:]]',
                            ' '
                        )
                    ),
                    ' ',
                    0
                ) AS from_amount
            FROM
                msg_atts p
                INNER JOIN msg_idx m
                ON p.tx_id = m.tx_id
                AND p.msg_group = m.msg_group
                AND p.msg_index = min_msg_index
            WHERE
                (
                    (
                        msg_type = 'token_swapped'
                        AND attribute_key = 'tokens_in'
                    )
                    OR (
                        msg_type = 'transfer'
                        AND attribute_key = 'amount'
                    )
                )
        ) {# ,
        fee_rec AS (
            SELECT
                p.tx_id,
                m.sender,
                m.recipient
            FROM
                pre_final p
                INNER JOIN (
                    SELECT
                        tx_id,
                        OBJECT_AGG(
                            attribute_key :: STRING,
                            attribute_value :: variant
                        ) AS j,
                        j :sender :: STRING AS sender,
                        j :recipient :: STRING AS recipient
                    FROM
                        msg_atts
                    WHERE
                        msg_type = 'transfer'
                        AND attribute_key IN (
                            'recipient',
                            'sender'
                        )
                        AND msg_group IS NULL
                    GROUP BY
                        tx_id
                ) m
                ON p.tx_id = m.tx_id
                AND p.trader = sender
        ),
        ex_protorev AS (
            SELECT
                A.tx_id,
                msg_index
            FROM
                msg_atts A
                JOIN fee_rec b
                ON A.tx_id = b.tx_id
                AND A.attribute_value = b.recipient
            WHERE
                msg_type = 'transfer'
                AND attribute_key = 'recipient'
        ) #},
        rel_to_transfers AS (
            SELECT
                A.tx_id,
                A.msg_index
            FROM
                msg_atts A
                JOIN pre_final b
                ON A.tx_id = b.tx_id
                AND A.attribute_value = b.trader
            WHERE
                msg_type = 'transfer'
                AND attribute_key = 'recipient'
        ),
        max_idx2 AS (
            SELECT
                p.tx_id,
                msg_group,
                MAX(
                    m.msg_index
                ) AS max_msg_index
            FROM
                pre_final p
                INNER JOIN msg_atts m
                ON p.tx_id = m.tx_id
                JOIN rel_to_transfers ex
                ON m.tx_id = ex.tx_id
                AND m.msg_index = ex.msg_index
            WHERE
                (
                    {# (
                    msg_type = 'token_swapped'
                    AND attribute_key = 'tokens_out'
                )
                OR #}(
                msg_type = 'transfer'
                AND attribute_key = 'amount'
        )
)
AND msg_group IS NOT NULL {# AND ex.tx_id IS NULL #}
GROUP BY
    p.tx_id,
    msg_group
),
to_amt AS (
    SELECT
        mm.tx_id,
        p.msg_index,
        mm.msg_group,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS to_currency,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS to_amount
    FROM
        msg_atts p
        INNER JOIN max_idx2 mm
        ON p.tx_id = mm.tx_id
        AND p.msg_group = mm.msg_group
        AND p.msg_index = max_msg_index
    WHERE
        (
            (
                msg_type = 'token_swapped'
                AND attribute_key = 'tokens_out'
            )
            OR (
                msg_type = 'transfer'
                AND attribute_key = 'amount'
            )
        )
),
pre_final2 AS (
    SELECT
        block_id,
        block_timestamp,
        p.tx_id,
        tx_succeeded,
        trader,
        COALESCE(TRY_CAST(f.from_amount AS bigint), TRY_CAST(p.from_amount AS bigint)) AS from_amount,
        COALESCE(
            f.from_currency,
            p.from_currency
        ) AS from_currency,
        CASE
            WHEN COALESCE(
                f.from_currency,
                p.from_currency
            ) LIKE 'gamm/pool/%' THEN 18
            ELSE l.decimal
        END AS from_decimal,
        tt.to_amount,
        tt.to_currency,
        CASE
            WHEN tt.to_currency LIKE 'gamm/pool/%' THEN 18
            ELSE A.decimal
        END AS TO_DECIMAL,
        pool_ids,
        p._inserted_timestamp,
        p._body_index
    FROM
        pre_final p
        LEFT OUTER JOIN pools pp
        ON p.tx_id = pp.tx_id
        AND p._body_index = pp._body_index
        LEFT OUTER JOIN from_amt f
        ON p.tx_id = f.tx_id
        AND p._body_index = f.msg_group
        LEFT OUTER JOIN to_amt tt
        ON p.tx_id = tt.tx_id
        AND p._body_index = tt.msg_group
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} A
        ON tt.to_currency = A.address
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
        l
        ON COALESCE(
            f.from_currency,
            p.from_currency
        ) = l.address
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    trader,
    from_amount :: NUMBER AS from_amount,
    from_currency,
    from_decimal,
    to_amount :: NUMBER AS to_amount,
    to_currency,
    TO_DECIMAL,
    pool_ids,
    _inserted_timestamp,
    _body_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','_body_index']
    ) }} AS swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final2
