{% macro usd_to_vnd_rate() %}
24000
{% endmacro %}

{% macro min_salary_offered(column_name) %}
CASE
    WHEN {{ column_name }} IS NULL OR trim({{ column_name }}) = '' THEN NULL
    WHEN lower({{ column_name }}) IN ('thoả thuận', 'thỏa thuận', 'negotiable') THEN NULL

    WHEN regexp_like(lower({{ column_name }}), '^\s*\d+\s*-\s*\d+') THEN
        CASE
            WHEN regexp_like(lower({{ column_name }}), 'usd') THEN
                TRY_CAST(regexp_extract({{ column_name }}, '(\d+)\s*-\s*\d+', 1) AS INTEGER)
                * {{ usd_to_vnd_rate() }}
            ELSE
                TRY_CAST(regexp_extract({{ column_name }}, '(\d+)\s*-\s*\d+', 1) AS INTEGER)
                * 1000000
        END

    WHEN regexp_like(lower({{ column_name }}), 'từ\s*\d+') THEN
        CASE
            WHEN regexp_like(lower({{ column_name }}), 'usd') THEN
                TRY_CAST(regexp_extract(lower({{ column_name }}), 'từ\s*(\d+)', 1) AS INTEGER)
                * {{ usd_to_vnd_rate() }}
            ELSE
                TRY_CAST(regexp_extract(lower({{ column_name }}), 'từ\s*(\d+)', 1) AS INTEGER)
                * 1000000
        END

    ELSE NULL
END
{% endmacro %}

{% macro max_salary_offered(column_name) %}
CASE
    WHEN {{ column_name }} IS NULL OR trim({{ column_name }}) = '' THEN NULL
    WHEN lower({{ column_name }}) IN ('thoả thuận', 'thỏa thuận', 'negotiable') THEN NULL

    WHEN regexp_like(lower({{ column_name }}), '^\s*\d+\s*-\s*\d+') THEN
        CASE
            WHEN regexp_like(lower({{ column_name }}), 'usd') THEN
                TRY_CAST(regexp_extract({{ column_name }}, '\d+\s*-\s*(\d+)', 1) AS INTEGER)
                * {{ usd_to_vnd_rate() }}
            ELSE
                TRY_CAST(regexp_extract({{ column_name }}, '\d+\s*-\s*(\d+)', 1) AS INTEGER)
                * 1000000
        END

    WHEN regexp_like(lower({{ column_name }}), '(upto|tới)\s*\d+') THEN
        CASE
            WHEN regexp_like(lower({{ column_name }}), 'usd') THEN
                TRY_CAST(regexp_extract(lower({{ column_name }}), '(upto|tới)\s*(\d+)', 2) AS INTEGER)
                * {{ usd_to_vnd_rate() }}
            ELSE
                TRY_CAST(regexp_extract(lower({{ column_name }}), '(upto|tới)\s*(\d+)', 2) AS INTEGER)
                * 1000000
        END

    ELSE NULL
END
{% endmacro %}

{% macro salary_value(column_min, column_max) %}
CASE
    WHEN {{ column_min }} IS NOT NULL AND {{ column_max }} IS NOT NULL THEN
        ROUND(({{ column_min }} + {{ column_max }}) / 2.0, 1)
    WHEN {{ column_min }} IS NOT NULL THEN {{ column_min }}
    WHEN {{ column_max }} IS NOT NULL THEN {{ column_max }}
    ELSE NULL
END
{% endmacro %}

{% macro salary_bench(column_name) %}
CASE
    WHEN {{ column_name }} < 10000000 THEN 'Below 10M VND'
    WHEN {{ column_name }} < 20000000 THEN '10-20M VND'
    WHEN {{ column_name }} < 30000000 THEN '20-30M VND'
    WHEN {{ column_name }} < 40000000 THEN '30-40M VND'
    WHEN {{ column_name }} < 50000000 THEN '40-50M VND'
    WHEN {{ column_name }} < 60000000 THEN '50-60M VND'
    WHEN {{ column_name }} < 70000000 THEN '60-70M VND'
    WHEN {{ column_name }} < 80000000 THEN '70-80M VND'
    WHEN {{ column_name }} < 90000000 THEN '80-90M VND'
    WHEN {{ column_name }} < 100000000 THEN '90-100M VND'
    WHEN {{ column_name }} >= 100000000 THEN 'Above 100M VND'
    ELSE 'Not Specified'
END
{% endmacro %}

{% macro salary_band(column_min, column_max) %}
CASE
    WHEN {{ column_min }} IS NULL AND {{ column_max }} IS NULL THEN 'Not Specified'

    WHEN {{ column_min }} IS NULL THEN
        CAST(
            CASE
                WHEN {{ column_max }} - 10000000 < 0 THEN 0
                ELSE ({{ column_max }} - 10000000) / 1000000
            END AS VARCHAR
        ) || '-' || CAST({{ column_max }} AS VARCHAR) || 'M VND'

    WHEN {{ column_max }} IS NULL THEN
        CAST({{ column_min }} / 1000000 AS VARCHAR) || '-' ||
        CAST(({{ column_min }} + 10000000) / 1000000 AS VARCHAR) || 'M VND'

    ELSE
        CAST({{ column_min }} / 1000000 AS VARCHAR) || '-' ||
        CAST({{ column_max }} / 1000000 AS VARCHAR) || 'M VND'
END
{% endmacro %}
