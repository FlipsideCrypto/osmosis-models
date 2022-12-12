{% macro udf_bulk_get_asset_metadata() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION silver.udf_bulk_get_asset_metadata() returns text api_integration = aws_osmosis_api_dev AS {% if target.database == 'OSMOSIS' %}
        'https://r9o2ijlhfc.execute-api.us-east-1.amazonaws.com/prod/get_asset_metadata'
    {% else %}
        'https://tirqv0b7h7.execute-api.us-east-1.amazonaws.com/dev/get_asset_metadata'
    {%- endif %}
{% endmacro %}