{% macro get_county_name(table_with_county_id) %}
    with county as (
        select
            county_id,
            lower(county_name) as county_name,
            native_id as county_abb
        from {{ ref('counties') }}
    ),
    station_with_county as (
        select
            {{ table_with_county_id }}.*,
            c.county_name,
            c.county_abb
        from {{ table_with_county_id }}
        inner join county as c
        on {{ table_with_county_id }}.county = c.county_id
    )

    select * from station_with_county
{% endmacro %}