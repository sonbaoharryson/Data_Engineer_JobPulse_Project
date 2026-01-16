{% macro extract_experience(description_col) %}

CASE

    /* Range: 3 - 5 years / năm */
    WHEN regexp_like({{ description_col }},
        '(?i)(tối thiểu|ít nhất|từ|from)?\s*\d+\s*(-|–|đến|to)\s*\d+\+?\s*(năm|years?)')
    THEN
        regexp_extract(
            {{ description_col }},
            '(?i)(tối thiểu|ít nhất|từ|from)?\s*((\d+)\s*(-|–|đến|to)\s*(\d+\+?))\s*(năm|years?)',
            2
        )


    /* Plus: 3+ years / năm */
    WHEN regexp_like({{ description_col }},
        '(?i)\d+\s*\+\s*(năm|years|year?)')
    THEN
        regexp_extract(
            {{ description_col }},
            '(?i)((\d+)\s*\+)\s*(năm|years?)',
            1
        )

    /* Single: 3 years / năm */

    -- Single: duoi/below/... 3 years => 3-
    WHEN regexp_like({{ description_col }},
        '(?i)(dưới|below|less than)\s*\d+\s*(năm|years?)')
    THEN
        concat(
            regexp_extract(
                {{ description_col }},
                '(?i)(dưới|below|less than)\s*(\d+)\s*(năm|years?)',
                2
            ),
            '-'
        )

    -- Single: at least/it nhat/... 3 years => 3+
    WHEN regexp_like({{ description_col }},
        '(?i)(tối thiểu|ít nhất|trên|at least|minimum)\s*\d+\s*(năm|years?)')
    THEN
        concat(
            regexp_extract(
                {{ description_col }},
                '(?i)(tối thiểu|ít nhất|trên|at least|minimum)\s*(\d+)\s*(năm|years?)',
                2
            ),
            '+'
        )

    -- Single: 3 years => 3+
    WHEN regexp_like({{ description_col }},
        '(?i)\d+\s*(năm|years?)')
    THEN
        regexp_extract(
            {{ description_col }},
            '(?i)(\d+)\s*(năm|years?)',
            1
        )

    ELSE 'Unknown'
END

{% endmacro %}