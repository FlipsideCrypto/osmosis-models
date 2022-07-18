{% macro udf_bulk_get_pool_balances() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_pool_balances() returns text api_integration = aws_osmosis_api_dev AS {% if target.name == "prod" -%}
        'https://k7jc1bnb8i.execute-api.us-east-1.amazonaws.com/prod/bulk_get_pool_balances'
    {% else %}
        'https://auacbjh2tj.execute-api.us-east-1.amazonaws.com/dev/bulk_get_pool_balances'
    {%- endif %}
{% endmacro %}
