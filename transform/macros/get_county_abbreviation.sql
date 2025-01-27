{% macro get_county_abbreviation(table_with_county_id) %}
    with county_abb as (
        select
            county_id,
            native_id
        from {{ ref('counties') }}
    ),
    station_with_county_abb as (
        select
            st.*,
            c.native_id
        from {{ table_with_county_id }} as st
        inner join county_abb as c
        on st..county = c.county_id
    )

    select * from station_with_county_abb
{% endmacro %}