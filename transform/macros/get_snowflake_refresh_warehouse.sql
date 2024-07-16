{% macro get_snowflake_refresh_warehouse(big="4XL", small="XS") %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% if flags.FULL_REFRESH %}
    {% set size = big %}
  {% else %}
    {% set size = small %}
  {% endif %}
  {% set warehouse = 'TRANSFORMING_' ~ size ~ '_' ~ suffix %}
  {{ return(warehouse) }}
{% endmacro %}
