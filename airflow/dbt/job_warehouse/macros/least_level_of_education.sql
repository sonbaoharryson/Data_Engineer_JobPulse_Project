{%macro least_level_of_education(column_name) %}
	CASE
		WHEN lower( {{column_name}} ) like '%cao đẳng%'
			OR lower( {{column_name}} ) like '%college%'
		THEN 'College'
		
		WHEN lower( {{column_name}} ) like '%đại học%'
			OR lower( {{column_name}} ) like '%bachelor%'
		THEN 'Bachelor'
		
		WHEN lower( {{column_name}} ) like '%thạc sĩ%'
			OR lower( {{column_name}} ) like '%master%'
		THEN 'Master'
		
		WHEN lower( {{column_name}} ) like '%tiến sĩ%'
			OR lower( {{column_name}} ) like '%phd%'
		THEN 'PhD'
		
	ELSE 'Unknown'
	END
{% endmacro %}