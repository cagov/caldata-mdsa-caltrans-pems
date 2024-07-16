{% macro unload_relation() %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% set partitioning = config.get('unload_partitioning', '') %}
  {% set stage = '@ANALYTICS_' ~ suffix ~ '.PUBLIC.PEMS_MARTS_' ~ suffix %}
  {% set key = model.name.split('_')[2:] | reject("eq", "") | join("_")  %}
  {% set key = key ~ ('' if partitioning else '.parquet') %}
  {% set path = this.schema ~ '/' ~ key %}
  {% set url = stage ~ '/' ~ path %}
      remove {{ url }};
      copy into {{ url }}
      from {{ this }}
      file_format = (type=parquet)
      {% if partitioning %}
          partition by {{ partitioning }}
          max_file_size = 134217728 -- 128 MiB
      {% else %}
          single = true
          max_file_size = 536870912 -- 512 MiB
      {% endif %}
      ;
{% endmacro %}
