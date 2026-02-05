{%macro location_normalization(column_name) %}
    -- Normalize job location by removing unwanted characters and standardizing format
    trim(
        replace(
            replace(
                lower({{ column_name }}),
                '(mới)',
            ''),
            '&',
        '-')
    )
{%endmacro %}