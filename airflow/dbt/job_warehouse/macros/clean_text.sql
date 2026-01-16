{% macro lower_and_trim(column_name) %}
    -- Normalize text: trim whitespace then lowercase
    lower(trim({{ column_name }}))
{% endmacro %}

{% macro initcap_and_trim(column_name) %}
    -- Normalize text: trim whitespace then initcap
    regexp_replace(lower(trim({{ column_name }})), '\b(\w)', x -> UPPER(x[1]))
{% endmacro %}

{% macro upper_and_trim(column_name) %}
    -- Normalize text: trim whitespace then uppercase
    upper(trim({{ column_name }}))
{% endmacro %}