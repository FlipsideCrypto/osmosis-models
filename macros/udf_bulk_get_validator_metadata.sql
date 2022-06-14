{% macro udf_bulk_get_validator_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_validator_metadata() returns text api_integration = aws_osmosis_api_dev AS {% if target.name == "prod" -%}
        'https://k7jc1bnb8i.execute-api.us-east-1.amazonaws.com/prod/get_validator_metadata'
    {% else %}
        'https://auacbjh2tj.execute-api.us-east-1.amazonaws.com/dev/get_validator_metadata'
    {%- endif %}
{% endmacro %}