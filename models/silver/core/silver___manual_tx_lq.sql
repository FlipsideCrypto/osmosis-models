{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge'
) }}

SELECT
    MD5(
        CAST(
            COALESCE(CAST(DATA :data :tx_response :height AS text), '' :: STRING) AS text
        )
    ) AS id,
    DATA :data :tx_response :height :: INT AS block_id,
    DATA :data :tx_response :timestamp :: timestamp_ntz AS block_timestamp,
    DATA :data :tx_response :codespace :: STRING AS codespace,
    DATA :data :tx_response :gas_used :: INT AS gas_used,
    DATA :data :tx_response :gas_wanted :: INT AS gas_wanted,
    DATA :data :tx_response :txhash :: STRING AS tx_id,
    CASE
        WHEN DATA :data :tx_response :code :: INT = 0 THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    DATA :data :tx_response :code :: INT tx_code,
    DATA :data :tx_response :events AS msgs,
    DATA :data :tx :auth_info AS auth_info,
    DATA :data :tx :body AS tx_body,
    block_id AS block_id_requested,
    _inserted_timestamp
FROM
    {{ source(
        'bronze_api',
        'manual_tx_lq'
    ) }}
WHERE
    block_id IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC) = 1)
