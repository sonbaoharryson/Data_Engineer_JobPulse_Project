{% macro extract_experience(description_col) %}

CASE

    /* Range: 3 - 5 years / năm */
    WHEN {{ description_col }} ~*
        '(tối thiểu|ít nhất|từ|from)?\s*\d+\s*(-|–|đến|to)\s*\d+\+?\s*(năm|years?)'
    THEN
        (regexp_match(
            {{ description_col }},
            '(tối thiểu|ít nhất|từ|from)?\s*((\d+)\s*(-|–|đến|to)\s*(\d+\+?))\s*(năm|years?)',
            'i'
        ))[2]


    /* Plus: 3+ years / năm */
    WHEN {{ description_col }} ~* 
        '\d+\s*\+\s*(năm|years|year?)'
    THEN
        (regexp_match(
            {{ description_col }},
            '((\d+)\s*\+)\s*(năm|years?)',
            'i'
        ))[1]

    /* Single: 3 years / năm */

    -- Single: duoi/below/... 3 years => 3-
    WHEN {{ description_col }} ~*
        '(dưới|below|less than)\s*\d+\s*(năm|years?)'
    THEN
        concat(
            (regexp_match(
                {{ description_col }},
                '(dưới|below|less than)\s*(\d+)\s*(năm|years?)',
                'i'
            ))[2],
            '-'
        )

    -- Single: at least/it nhat/... 3 years => 3+
    WHEN {{ description_col }} ~*
        '(tối thiểu|ít nhất|trên|at least|minimum)\s*\d+\s*(năm|years?)'
    THEN
        concat(
            (regexp_match(
                {{ description_col }},
                '(tối thiểu|ít nhất|trên|at least|minimum)\s*(\d+)\s*(năm|years?)',
                'i'
            ))[2],
            '+'
        )

    -- Single: 3 years => 3+
    WHEN {{ description_col }} ~*
        '\d+\s*(năm|years?)'
    THEN
        (regexp_match(
            {{ description_col }},
            '(\d+)\s*(năm|years?)',
            'i'
        ))[1]

    ELSE 'Unknown'
END

{% endmacro %}
