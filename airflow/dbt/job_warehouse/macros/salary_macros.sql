{% macro min_salary_offered(column_name) %}
    -- Extract minimum salary in million VND from salary text
    CASE
        WHEN {{ column_name}} IS NULL OR trim({{ column_name}}) = '' THEN NULL
        WHEN lower({{ column_name}}) IN ('thoả thuận', 'negotiable') THEN NULL
        WHEN regexp_like(lower({{ column_name}}), '^\s*\d+\s*-\s*\d+') THEN
            TRY_CAST(regexp_extract({{ column_name}}, '(\d+)\s*-\s*\d+', 1) AS INTEGER)
        WHEN regexp_like(lower({{ column_name}}), 'từ\s*\d+') THEN
            TRY_CAST(regexp_extract(lower({{ column_name}}), 'từ\s*(\d+)', 1) AS INTEGER)
        ELSE NULL
    END
{% endmacro %}

{% macro max_salary_offered(column_name) %}
    -- Extract maximum salary in million VND from salary text
    CASE
        WHEN {{ column_name}} IS NULL OR trim({{ column_name}}) = '' THEN NULL
        WHEN lower({{ column_name}}) IN ('thoả thuận', 'negotiable') THEN NULL
        WHEN regexp_like(lower({{ column_name}}), '^\s*\d+\s*-\s*\d+') THEN
            TRY_CAST(regexp_extract({{ column_name}}, '\d+\s*-\s*(\d+)', 1) AS INTEGER)
        WHEN regexp_like(lower({{ column_name}}), '(upto|tới)\s*\d+') THEN
            TRY_CAST(regexp_extract(lower({{ column_name}}), '(upto|tới)\s*(\d+)', 2) AS INTEGER)   
        ELSE NULL
    END
{% endmacro %}

{% macro salary_value(column_1, column_2) %}
    CASE
        WHEN {{ column_1 }} IS NOT NULL AND {{ column_2 }} IS NOT NULL THEN
            ROUND(({{ column_1 }} + {{ column_2 }}) / 2.0, 1)

        WHEN {{ column_1 }} IS NOT NULL THEN {{ column_1 }}
        WHEN {{ column_2 }} IS NOT NULL THEN {{ column_2 }}
        ELSE NULL
    END
{% endmacro %}


{%macro salary_bench(column_name) %}
    CASE
        WHEN {{ column_name }} < 10 THEN 'Below 10M VND'
        WHEN {{ column_name }} >= 10 AND {{ column_name }} < 20 THEN '10-20M VND'
        WHEN {{ column_name }} >= 20 AND {{ column_name }} < 30 THEN '20-30M VND'
        WHEN {{ column_name }} >= 30 AND {{ column_name }} < 40 THEN '30-40M VND'
        WHEN {{ column_name }} >= 40 AND {{ column_name }} < 50 THEN '40-50M VND'
        WHEN {{ column_name }} >= 50 AND {{ column_name }} < 60 THEN '50-60M VND'
        WHEN {{ column_name }} >= 30 AND {{ column_name }} < 40 THEN '60-70M VND'
        WHEN {{ column_name }} >= 70 AND {{ column_name }} < 80 THEN '70-80M VND'
        WHEN {{ column_name }} >= 80 AND {{ column_name }} < 90 THEN '80-90M VND'
        WHEN {{ column_name }} >= 90 AND {{ column_name }} < 100 THEN '90-100M VND'
        WHEN {{ column_name }} >= 100 THEN 'Above 100M VND'
        ELSE 'Not Specified'
    END
{% endmacro %}

{% macro salary_band(column_1, column_2) %}
    CASE
        WHEN {{ column_1 }} IS NULL AND {{ column_2 }} IS NULL THEN 'Not Specified'
        WHEN {{ column_1 }} IS NULL AND {{ column_2 }} IS NOT NULL THEN
            CAST(IF({{ column_2 }} - 10 < 0, 0, {{ column_2 }} - 10) AS VARCHAR) || '-' ||
            CAST({{ column_2 }} AS VARCHAR) || 'M VND'
        WHEN {{ column_1 }} IS NOT NULL AND {{ column_2 }} IS NULL THEN
            CAST({{ column_1 }} AS VARCHAR) || '-' ||
            CAST({{ column_1 }} + 10 AS VARCHAR) || 'M VND'
        ELSE
            CAST({{ column_1 }} AS VARCHAR) || '-' ||
            CAST({{ column_2 }} AS VARCHAR) || 'M VND'
    END
{% endmacro %}
