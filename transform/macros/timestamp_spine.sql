{% macro timestamp_spine(start_date, end_date, second_increment=60*60*24) %}

  {# Do not allow increments of less than 1 or longer than a full day #}
  {% if second_increment <= 0 or second_increment > 60*60*24 %}
    {% set second_increment = 60*60*24 %}
  {% endif %}

  with date_spine as (
    {{ dbt_utils.date_spine(datepart="day", start_date=start_date, end_date=end_date) }}
  ),

  series as (
    {{ dbt_utils.generate_series(60*60*24 / second_increment) }}
  )

  select DATEADD(s, (generated_number - 1) * {{ second_increment }}, date_day) as timestamp_column
  from date_spine
  cross join series

{% endmacro %}
