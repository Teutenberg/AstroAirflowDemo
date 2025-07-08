{% macro extract_json_field(json_col, key, type=None) %}
    {%- if target.type == 'postgres' -%}
        {%- if type is none -%}
            {{ json_col }}->>'{{ key }}'
        {%- else -%}
            ({{ json_col }}->>'{{ key }}')::{{ type | lower }}
        {%- endif -%}
    {%- elif target.type == 'snowflake' -%}
        {%- if type is none -%}
            ({{ json_col }}:{{ key }})
        {%- else -%}
            ({{ json_col }}:{{ key }})::{{ type | lower }}
        {%- endif -%}
    {%- elif target.type in ['databricks', 'spark'] -%}
        {%- if type is none -%}
            get_json_object({{ json_col }}, '$.{{ key }}')
        {%- else -%}
            CAST(get_json_object({{ json_col }}, '$.{{ key }}') AS {{ type | upper }})
        {%- endif -%}
    {%- elif target.type == 'bigquery' -%}
        {%- if type is none -%}
            JSON_VALUE({{ json_col }}, '$.{{ key }}')
        {%- else -%}
            CAST(JSON_VALUE({{ json_col }}, '$.{{ key }}') AS {{ type | upper }})
        {%- endif -%}
    {%- elif target.type == 'redshift' -%}
        {%- if type is none -%}
            {{ json_col }}->>'{{ key }}'
        {%- else -%}
            ({{ json_col }}->>'{{ key }}')::{{ type | lower }}
        {%- endif -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("extract_json_field macro is not implemented for this database: " ~ target.type) }}
    {%- endif -%}
{% endmacro %}