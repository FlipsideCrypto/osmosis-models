{% macro create_aws_osmosis_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_api_prod api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::924682671219:role/osmosis-api-prod-rolesnowflakeudfsAF733095-4rS81vtCwRvD' api_allowed_prefixes = (
            'https://9ybmmw2jk7.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_osmosis_api_stg api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/osmosis-api-stg-rolesnowflakeudfsAF733095-HJ3v3z0NQBus' api_allowed_prefixes = (
            'https://sz3utstqtb.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
