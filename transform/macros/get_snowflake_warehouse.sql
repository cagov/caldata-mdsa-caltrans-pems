{% macro get_snowflake_warehouse(size="XS" %}
  {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% set warehouse = 'TRANSFORMING_' ~ size ~ '_' ~ suffix %}
  {{ return(warehouse) }}
{% endmacro %}
