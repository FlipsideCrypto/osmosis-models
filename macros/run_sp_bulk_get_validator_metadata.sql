{% macro run_sp_bulk_get_validator_metadata(db_name) %}
{% set sql %}
call {{ db_name }}.silver.sp_bulk_get_validator_metadata();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}