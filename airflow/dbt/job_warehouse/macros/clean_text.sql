{% macro lower_and_trim(column_name) %}
    -- Normalize text: trim whitespace then lowercase
    lower(trim({{ column_name }}))
{% endmacro %}

{% macro initcap_and_trim(column_name) %}
    -- Normalize text: trim whitespace then initcap
    initcap(trim({{ column_name }}))
{% endmacro %}

{% macro upper_and_trim(column_name) %}
    -- Normalize text: trim whitespace then uppercase
    upper(trim({{ column_name }}))
{% endmacro %}