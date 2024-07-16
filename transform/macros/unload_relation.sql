{% macro unload_relation() %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% set stage = '@ANALYTICS_' ~ suffix ~ '.PUBLIC.PEMS_MARTS_' ~ suffix %}
  {% set object_name = model.name.split('_')[2:] | reject("eq", "") | join("_")  %}
  {% set object_name = object_name ~ '.parquet' if config.get('unload_single', True) else '' %}
  {% set path = this.schema ~ '/' ~ object_name %}
  {% set url = stage ~ '/' ~ path %}
  copy into {{ url }}
  from {{ this }}
  file_format = (type=parquet)
  {% if config.get('unload_single', True) %}
      single = true
      max_file_size = 536870912 -- 512 MiB
      overwrite = true
  {% endif %}
{% endmacro %}
