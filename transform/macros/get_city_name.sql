{% macro get_city_name(table_with_city_id) %}
    with city as (
        select
            city_id,
            city_name,
            native_id
        from {{ ref('cities') }}
    ),
    station_with_city_id as (
        select
            st.*,
            c.city_name,
            c.native_id as city_abb
        from {{ table_with_city_id }} as st
        inner join city as c
        on st.city = c.city_id
    )

    select * from station_with_city_id
{% endmacro %}