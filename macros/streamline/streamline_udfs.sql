{% macro create_udf_get_chainhead() %}
    {% if target.name == "prod" %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_osmosis_api AS 'https://99iu3zvgd9.execute-api.us-east-1.amazonaws.com/prod/get_chainhead'
    {% else %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_osmosis_dev_api AS 'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    {% if target.name == "prod" %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_api AS 'https://99iu3zvgd9.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_json_rpc'
    {% else %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_dev_api AS 'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_rest_api() %}
    {% if target.name == "prod" %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_api AS 'https://99iu3zvgd9.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api' {% elif target.name == "serverless-prod" %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
            json OBJECT
        ) returns variant api_integration = aws_osmosis_api_prod AS 'https://9ybmmw2jk7.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api' {% elif target.name == "serverless-stg" %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
            json OBJECT
        ) returns variant api_integration = aws_osmosis_api_stg AS 'https://sz3utstqtb.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {% else %}
        CREATE
        OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_dev_api AS 'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}
