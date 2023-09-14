{% macro create_udf_get_chainhead() %}
    {% if target.name == "prod" %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_osmosis_api AS 
            'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'
    {% else %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_get_chainhead() returns variant api_integration = aws_osmosis_dev_api AS 
            'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/get_chainhead'  
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_json_rpc() %}
    {% if target.name == "prod" %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_api AS 
            'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {% else %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_json_rpc(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_dev_api AS 
            'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_json_rpc'
    {%- endif %};
{% endmacro %}

{% macro create_udf_bulk_rest_api() %}
    {% if target.name == "prod" %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_api AS 
            'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_rest_api'
    {% else %}
        CREATE OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api(
            json OBJECT
        ) returns ARRAY api_integration = aws_osmosis_dev_api AS 
            'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}