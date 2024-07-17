{% macro unload_relation(strip_leading_words=1, unload_partitioning=none) %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% set partitioning = unload_partitioning or config.get('unload_partitioning', none) %}
  {% set stage = '@ANALYTICS_' ~ suffix ~ '.PUBLIC.PEMS_MARTS_' ~ suffix %}
  {% set key = model.name.split('_')[strip_leading_words:] | reject("eq", "") | join("_")  %}
  {% set key = key ~ ('' if partitioning else '.parquet') %}
  {% set path = this.schema ~ '/' ~ key %}
  {% set url = stage ~ '/' ~ path %}
      remove {{ url }};
      copy into {{ url }}
      from {{ this }}
      {% if partitioning %}
          partition by {{ partitioning }}
      {% endif %}
      file_format = (type=parquet)
      {% if partitioning %}
          max_file_size = 134217728 -- 128 MiB
      {% else %}
          single = true
          max_file_size = 536870912 -- 512 MiB
      {% endif %}
      ;
{% endmacro %}
