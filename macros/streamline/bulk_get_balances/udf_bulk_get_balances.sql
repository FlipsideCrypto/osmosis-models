{% macro udf_bulk_get_balances() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_balances() returns text api_integration = aws_osmosis_api_dev AS {% if target.database == 'OSMOSIS' %}
        'https://r9o2ijlhfc.execute-api.us-east-1.amazonaws.com/prod/bulk_get_balances'
    {% else %}
        'https://tirqv0b7h7.execute-api.us-east-1.amazonaws.com/dev/bulk_get_balances'
    {%- endif %}
{% endmacro %}
