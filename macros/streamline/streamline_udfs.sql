{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_osmosis_api_prod AS 'https://9ybmmw2jk7.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
        aws_osmosis_api_stg AS 'https://sz3utstqtb.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}
