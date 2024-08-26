with aadt as (
    select *
    from {{ ref('int_performance__station_aadt_with_K_value') }}
)

select * from aadt
