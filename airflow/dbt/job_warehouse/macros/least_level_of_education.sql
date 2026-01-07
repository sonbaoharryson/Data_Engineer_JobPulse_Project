{%macro least_level_of_education(column_name) %}
	CASE
		WHEN lower( {{column_name}} ) ilike '%cao đẳng%'
			OR lower( {{column_name}} ) ilike '%college%'
		THEN 'College'
		
		WHEN lower( {{column_name}} ) ilike '%đại học%'
			OR lower( {{column_name}} ) ilike '%bachelor%'
		THEN 'Bachelor'
		
		WHEN lower( {{column_name}} ) ilike '%thạc sĩ%'
			OR lower( {{column_name}} ) ilike '%master%'
		THEN 'Master'
		
		WHEN lower( {{column_name}} ) ilike '%tiến sĩ%'
			OR lower( {{column_name}} ) ilike '%phd%'
		THEN 'PhD'
		
	ELSE 'Unknown'
	END
{% endmacro %}