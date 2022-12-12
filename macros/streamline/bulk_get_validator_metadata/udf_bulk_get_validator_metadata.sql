{% macro udf_bulk_get_validator_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_validator_metadata() returns text api_integration = aws_osmosis_api_dev AS {% if target.database == 'OSMOSIS' %}
        'https://r9o2ijlhfc.execute-api.us-east-1.amazonaws.com/prod/get_validator_metadata'
    {% else %}
        'https://tirqv0b7h7.execute-api.us-east-1.amazonaws.com/dev/get_validator_metadata'
    {%- endif %}
{% endmacro %}