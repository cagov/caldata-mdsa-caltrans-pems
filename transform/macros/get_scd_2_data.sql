
{% macro get_scd_2_data(active_date,_valid_from,_valid_to) %}

    {{ active_date }} >= {{ _valid_from }}
    and ( {{ active_date }} < {{ _valid_to }} or {{ _valid_to }} is null)

{% endmacro %}