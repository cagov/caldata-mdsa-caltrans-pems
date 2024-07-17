{% macro get_snowflake_warehouse(size="XS") %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% set warehouse = 'TRANSFORMING_' ~ size ~ '_' ~ suffix %}
  {{ return(warehouse) }}
{% endmacro %}
