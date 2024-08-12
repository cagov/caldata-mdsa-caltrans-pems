{% macro unload_relation_as_geojson(strip_leading_words=1) %}
  {% if target.name == 'prd' %}
    {% set suffix = 'PRD' %}
  {% else %}
    {% set suffix = 'DEV' %}
  {% endif %}
  {% set stage = '@ANALYTICS_' ~ suffix ~ '.PUBLIC.PEMS_MARTS_' ~ suffix %}
  {% set database = "ANALYTICS_" ~ suffix %}
  {% set name = model.name.split('_')[strip_leading_words:] | reject("eq", "") | join("_") %}
      /* Note the unusual quoting of the arguments to the stored procedure.
         This is because it is passed as strings to the Python session.
         The dbt docs say never to construct a relation this way, but I'm not
         sure whether it's possible to do it using the adapter.get_relation() macro...
         */
      call {{ database }}.public.unload_as_geojson(
          '{{ this.database ~ "." ~ this.schema ~ "." ~ this.table }}',
          '{{ name }}',
          '{{ stage ~ '/' ~ this.schema }}'
      );
{% endmacro %}
