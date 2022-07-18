{% macro run_sp_bulk_get_balances() %}
{% set sql %}
call silver.sp_bulk_get_balances();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}