{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_asset_metadata() }};
    {{ udf_bulk_get_balances() }};
    {{ udf_bulk_get_validator_metadata() }};
    {{ udf_bulk_get_pool_balances()}};
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
