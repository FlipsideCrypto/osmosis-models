{% macro create_aws_osmosis_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/osmosis-api-prod-rolesnowflakeudfsAF733095-LHZ76F0KPYOE' api_allowed_prefixes = (
            'https://99iu3zvgd9.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "serverless-prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_api_prod api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/osmosis-api-prod-rolesnowflakeudfsAF733095-4rS81vtCwRvD' api_allowed_prefixes = (
            'https://9ybmmw2jk7.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "serverless-stg" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_api_stg api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/osmosis-api-stg-rolesnowflakeudfsAF733095-HJ3v3z0NQBus' api_allowed_prefixes = (
            'https://sz3utstqtb.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% else %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_dev_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/osmosis-api-dev-rolesnowflakeudfsAF733095-ZCB8M2UY95LZ' api_allowed_prefixes = (
            'https://sz3utstqtb.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}