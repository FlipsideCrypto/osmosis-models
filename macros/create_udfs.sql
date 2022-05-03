{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_asset_metadata() }};
{# Add crate udf macros here #}
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
