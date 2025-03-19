{% macro write_iceberg_metadata() %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% set stage = '@ANALYTICS_' ~ suffix ~ '.PUBLIC.PEMS_MARTS_' ~ suffix %}
  {% set database = "ANALYTICS_" ~ suffix %}
  {% set query %}
      call {{ database }}.public.write_iceberg_metadata(
          '{{ database }}',
          '{{ stage }}'
      );
  {% endset %}
  {{ log('Writing iceberg metadata to ' ~ stage, info=true) }}
  {{ run_query(query) }}
{% endmacro %}
