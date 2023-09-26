{% macro create_aws_osmosis_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/osmosis-api-prod-rolesnowflakeudfsAF733095-LHZ76F0KPYOE' api_allowed_prefixes = (
            'https://99iu3zvgd9.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_dev_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/osmosis-api-dev-rolesnowflakeudfsAF733095-ZCB8M2UY95LZ' api_allowed_prefixes = (
            'https://8lng1cjnel.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}